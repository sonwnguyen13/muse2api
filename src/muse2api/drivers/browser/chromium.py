"""Chromium process management."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit

import httpx

log = logging.getLogger(__name__)

_CANDIDATES = (
    "chromium",
    "chromium-browser",
    "google-chrome",
    "google-chrome-stable",
    "chrome",
    "/usr/bin/chromium",
    "/snap/bin/chromium",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
    os.path.expandvars(r"%LOCALAPPDATA%\CocCoc\Browser\Application\browser.exe"),
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
)


def parse_proxy_spec(raw: str) -> tuple[str, int, str, str]:
    """Parse ``ip:port:user:pass`` or ``http://user:pass@ip:port`` into ``(host, port, user, pass)``."""
    s = (raw or "").strip()
    if not s:
        return "", 0, "", ""
    if "://" not in s:
        parts = s.split(":")
        if len(parts) == 4:
            host, port, user, pw = parts
            return host, int(port), user, pw
        if len(parts) == 2:
            host, port = parts
            return host, int(port), "", ""
        s = f"http://{s}"
    parsed = urlsplit(s)
    host = parsed.hostname or ""
    port = int(parsed.port or 80)
    user = parsed.username or ""
    pw = parsed.password or ""
    return host, port, user, pw


def find_chromium(configured: str = "") -> str:
    for c in ((configured,) if configured else ()) + _CANDIDATES:
        if c and (os.path.isfile(c) or shutil.which(c)):
            return c
    raise FileNotFoundError(
        "Chromium/Chrome not found; install it or set MUSE2API_CHROMIUM_PATH"
    )


class _LocalProxyBridge:
    """Local HTTP/CONNECT proxy on 127.0.0.1 that injects Proxy-Authorization for upstream."""

    def __init__(self, up_host: str, up_port: int, username: str, password: str) -> None:
        self.up_host = up_host
        self.up_port = up_port
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        self._auth_header = f"Proxy-Authorization: Basic {token}\r\n".encode()
        self._server: asyncio.AbstractServer | None = None
        self.local_port: int = 0

    async def start(self) -> str:
        self._server = await asyncio.start_server(self._handle_client, "127.0.0.1", 0)
        sock = self._server.sockets[0]
        self.local_port = sock.getsockname()[1]
        log.info("started local proxy auth bridge on 127.0.0.1:%d -> %s:%d",
                 self.local_port, self.up_host, self.up_port)
        return f"http://127.0.0.1:{self.local_port}"

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                writer.write(chunk)
                await writer.drain()
        except Exception:
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    async def _handle_client(self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
        up_w = None
        try:
            head = await asyncio.wait_for(client_r.readuntil(b"\r\n\r\n"), timeout=15.0)
            up_r, up_w = await asyncio.wait_for(
                asyncio.open_connection(self.up_host, self.up_port), timeout=15.0
            )
            patched = head[:-2] + self._auth_header + b"\r\n"
            up_w.write(patched)
            await up_w.drain()
            await asyncio.gather(self._pipe(client_r, up_w), self._pipe(up_r, client_w))
        except Exception:
            with contextlib.suppress(Exception):
                client_w.close()
            if up_w:
                with contextlib.suppress(Exception):
                    up_w.close()


class ChromiumProcess:
    def __init__(self, executable: str, port: int, profile_dir: Path, headless: bool = True,
                 proxy: str = ""):
        self.executable = executable
        self.port = port
        self.profile_dir = Path(profile_dir).resolve()
        self.headless = headless
        self.up_host, self.up_port, self.proxy_username, self.proxy_password = parse_proxy_spec(proxy)
        self.proxy = f"http://{self.up_host}:{self.up_port}" if self.up_host else ""
        self._bridge: _LocalProxyBridge | None = None
        self.proc: asyncio.subprocess.Process | None = None

    @property
    def http_base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def browser_ws_url(self) -> str | None:
        try:
            # Local DevTools port must not follow http_proxy/https_proxy.
            async with httpx.AsyncClient(timeout=2, trust_env=False) as client:
                r = await client.get(f"{self.http_base}/json/version")
                return r.json().get("webSocketDebuggerUrl")
        except (httpx.HTTPError, ValueError):
            return None

    async def start(self) -> str:
        """Start (or attach to an already running) Chromium; return the browser ws URL."""
        if url := await self.browser_ws_url():
            if not (self.up_host and self.proxy_username and self._bridge is None):
                log.info("attached to existing Chromium on port %s", self.port)
                return url
            log.info("closing orphaned Chromium on port %s to attach fresh proxy bridge", self.port)
            try:
                from muse2api.drivers.browser.cdp import CDPConnection
                cdp = await CDPConnection.connect(url)
                await cdp.send("Browser.close")
                await cdp.close()
                for _ in range(10):
                    await asyncio.sleep(0.3)
                    if not await self.browser_ws_url():
                        break
            except Exception:
                pass

        effective_proxy = self.proxy
        if self.up_host and self.proxy_username:
            self._bridge = _LocalProxyBridge(
                self.up_host, self.up_port, self.proxy_username, self.proxy_password
            )
            effective_proxy = await self._bridge.start()

        self.profile_dir.mkdir(parents=True, exist_ok=True)
        for lock in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
            with contextlib.suppress(FileNotFoundError):
                (self.profile_dir / lock).unlink()

        args = [
            self.executable,
            f"--remote-debugging-port={self.port}",
            "--remote-allow-origins=*",
            f"--user-data-dir={self.profile_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-dev-shm-usage",
            "--disable-background-networking",
            "--autoplay-policy=no-user-gesture-required",
            "--window-size=1440,2200",
            "about:blank",
        ]
        if effective_proxy:
            args.insert(1, "--proxy-bypass-list=<-loopback>")
            args.insert(1, f"--proxy-server={effective_proxy}")
        if self.headless:
            args[1:1] = ["--headless=new", "--disable-gpu"]
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            args.insert(1, "--no-sandbox")

        log.info("launching Chromium: %s (proxy=%s)", self.executable, effective_proxy or "none")
        self.proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        for _ in range(60):
            if url := await self.browser_ws_url():
                return url
            if self.proc.returncode is not None:
                break
            await asyncio.sleep(0.5)
        raise RuntimeError("Chromium did not expose a DevTools endpoint")

    async def stop(self) -> None:
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), 10)
            except asyncio.TimeoutError:
                self.proc.kill()
        self.proc = None
        if self._bridge:
            await self._bridge.stop()
            self._bridge = None

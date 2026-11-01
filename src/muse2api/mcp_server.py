"""MCP Server for muse2api (Model Context Protocol stdio / SSE server).

Run with:
    python -m muse2api.mcp_server
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("muse2api")

MUSE2API_URL = os.environ.get("MUSE2API_URL", "http://127.0.0.1:18610").rstrip("/")
MUSE2API_KEY = os.environ.get("MUSE2API_API_KEY", os.environ.get("MUSE2API_KEY", "muse2api-local"))


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {MUSE2API_KEY}", "Content-Type": "application/json"}


@mcp.tool()
async def muse_status() -> dict[str, Any]:
    """Check muse2api gateway readiness and active accounts."""
    async with httpx.AsyncClient(timeout=15.0, trust_env=False) as cl:
        ready = (await cl.get(f"{MUSE2API_URL}/readyz")).json()
        accs = (await cl.get(f"{MUSE2API_URL}/admin/accounts", headers=_headers())).json()
    return {"base_url": MUSE2API_URL, "readyz": ready, "accounts": accs.get("data", [])}


@mcp.tool()
async def muse_quota(refresh: bool = False) -> dict[str, Any]:
    """Check weekly usage percentage and remaining extra tokens (`1B tokens left`) for all Muse accounts."""
    suffix = "?refresh=1" if refresh else ""
    async with httpx.AsyncClient(timeout=45.0, trust_env=False) as cl:
        accs = (await cl.get(f"{MUSE2API_URL}/admin/accounts", headers=_headers())).json().get("data", [])
        out = []
        for a in accs:
            aid = a["id"]
            r = await cl.get(f"{MUSE2API_URL}/admin/accounts/{aid}/quota{suffix}", headers=_headers())
            out.append({"id": aid, "label": a.get("label"), "quota": r.json().get("quota")})
    return {"accounts": out}


@mcp.tool()
async def muse_generate_image(
    prompt: str,
    size: str = "16:9",
    output_path: str = "",
    reference_images: list[str] | None = None,
) -> dict[str, Any]:
    """Generate an image via muse2api (`muse-image`)."""
    body: dict[str, Any] = {
        "model": "muse-image",
        "prompt": prompt,
        "size": size,
        "response_format": "url",
        "async": True,
    }
    if reference_images:
        body["image"] = reference_images
    async with httpx.AsyncClient(timeout=60.0, trust_env=False) as cl:
        r = await cl.post(f"{MUSE2API_URL}/v1/images/generations", headers=_headers(), json=body)
        r.raise_for_status()
        task = r.json()
        tid = task["id"]
        for _ in range(120):
            await asyncio.sleep(2.0)
            pr = await cl.get(f"{MUSE2API_URL}/v1/images/generations/{tid}", headers=_headers())
            pdata = pr.json()
            st = pdata.get("status")
            if st == "succeeded":
                url = pdata["result"]["data"][0]["url"]
                if url.startswith("/"):
                    url = f"{MUSE2API_URL}{url}"
                saved = ""
                if output_path:
                    p = Path(output_path)
                    p.parent.mkdir(parents=True, exist_ok=True)
                    dl = await cl.get(url)
                    p.write_bytes(dl.content)
                    saved = str(p.resolve())
                return {"ok": True, "task_id": tid, "url": url, "saved_path": saved}
            if st in ("failed", "cancelled"):
                return {"ok": False, "task_id": tid, "error": pdata.get("error")}
    return {"ok": False, "error": "Image generation timed out"}


@mcp.tool()
async def muse_generate_video(
    prompt: str,
    size: str = "16:9",
    duration: int = 8,
    image_url: str = "",
    output_path: str = "",
) -> dict[str, Any]:
    """Generate a video via muse2api (`muse-video`)."""
    body: dict[str, Any] = {
        "model": "muse-video",
        "prompt": prompt,
        "size": size,
        "duration": duration,
    }
    if image_url:
        body["image"] = image_url
    async with httpx.AsyncClient(timeout=60.0, trust_env=False) as cl:
        r = await cl.post(f"{MUSE2API_URL}/v1/videos", headers=_headers(), json=body)
        r.raise_for_status()
        task = r.json()
        tid = task["id"]
        for _ in range(180):
            await asyncio.sleep(3.0)
            pr = await cl.get(f"{MUSE2API_URL}/v1/videos/{tid}", headers=_headers())
            pdata = pr.json()
            st = pdata.get("status")
            if st == "succeeded":
                url = pdata["result"]["url"]
                if url.startswith("/"):
                    url = f"{MUSE2API_URL}{url}"
                saved = ""
                if output_path:
                    p = Path(output_path)
                    p.parent.mkdir(parents=True, exist_ok=True)
                    dl = await cl.get(url)
                    p.write_bytes(dl.content)
                    saved = str(p.resolve())
                return {"ok": True, "task_id": tid, "url": url, "saved_path": saved}
            if st in ("failed", "cancelled"):
                return {"ok": False, "task_id": tid, "error": pdata.get("error")}
    return {"ok": False, "error": "Video generation timed out"}


@mcp.tool()
async def muse_accounts(
    action: str = "list",
    label: str = "",
    cookies_json: str = "",
    account_id: str = "",
) -> dict[str, Any]:
    """Manage accounts in muse2api (`list`, `add`, `reset`, `renew`, `delete`)."""
    act = (action or "list").strip().lower()
    async with httpx.AsyncClient(timeout=30.0, trust_env=False) as cl:
        if act == "list":
            return (await cl.get(f"{MUSE2API_URL}/admin/accounts", headers=_headers())).json()
        if act == "add":
            cks = json.loads(cookies_json) if cookies_json.strip().startswith("{") else cookies_json
            r = await cl.post(
                f"{MUSE2API_URL}/admin/accounts",
                headers=_headers(),
                json={"label": label or f"acc_{int(time.time())}", "cookies": cks},
            )
            return r.json()
        if act in ("reset", "renew") and account_id:
            return (await cl.post(f"{MUSE2API_URL}/admin/accounts/{account_id}/{act}", headers=_headers())).json()
        if act == "delete" and account_id:
            return (await cl.delete(f"{MUSE2API_URL}/admin/accounts/{account_id}", headers=_headers())).json()
    return {"ok": False, "error": f"Invalid action={action!r}"}


if __name__ == "__main__":
    mcp.run()

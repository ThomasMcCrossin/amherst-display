"""One OpenAI-shaped vision chat call, routed by endpoint and model (Tom, 2026-11-03).

Callers build an OpenAI chat body (text and image_url data-URL parts) and get back an
OpenAI-shaped payload ({"choices": [{"message": {"content": ...}}], "usage": {...}}), whichever
backend answered:

- base_url on ollama.com: the native /api/chat, so reasoning can be set. SCOREBUG_VISION_THINK
  (default "low"; "off" disables) - ollama's /v1 endpoint ignores a low setting and DeepSeek Flash
  ran 22k reasoning tokens / 69 s per call there.
- a claude-* model: the Anthropic Messages API (base_url https://api.anthropic.com), with
  ANTHROPIC_WORKSPACE_ID as the anthropic-workspace-id header when set.
- anything else: POST {base_url}/chat/completions unchanged.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List

ANTHROPIC_VERSION = "2023-06-01"


def _parts(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    content = body["messages"][-1]["content"]
    return [{"type": "text", "text": content}] if isinstance(content, str) else list(content)


def _b64(part: Dict[str, Any]) -> str:
    return part["image_url"]["url"].split(",", 1)[1]


def _json_text(text: str) -> str:
    """The reply's JSON object (models wrap it in ``` fences or add a sentence)."""
    m = re.search(r"\{.*\}", text, re.S)
    return m.group(0) if m else text


def _ollama(base_url: str, api_key: str, body: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    import requests

    # Native messages take one string and a list of images, so each text part and the images
    # after it become one message; the labels stay next to their frames.
    messages: List[Dict[str, Any]] = []
    for p in _parts(body):
        if p["type"] == "text":
            messages.append({"role": "user", "content": p["text"], "images": []})
        else:
            if not messages:
                messages.append({"role": "user", "content": "", "images": []})
            messages[-1]["images"].append(_b64(p))
    think = os.environ.get("SCOREBUG_VISION_THINK", "low")
    root = base_url.split("/v1")[0].rstrip("/")
    resp = requests.post(f"{root}/api/chat", timeout=timeout, headers={"Authorization": f"Bearer {api_key}"}, json={
        "model": body["model"], "stream": False, "format": "json", "options": {"temperature": 0},
        "think": False if think == "off" else think, "messages": messages})
    resp.raise_for_status()
    r = resp.json()
    return {"choices": [{"message": {"content": _json_text(r["message"]["content"])}}],
            "usage": {"prompt_tokens": r.get("prompt_eval_count") or 0, "completion_tokens": r.get("eval_count") or 0}}


def _anthropic(base_url: str, api_key: str, body: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    import requests

    content = [{"type": "text", "text": p["text"]} if p["type"] == "text" else
               {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": _b64(p)}}
               for p in _parts(body)]
    content.append({"type": "text", "text": "Reply with the JSON object only."})
    headers = {"x-api-key": api_key, "anthropic-version": ANTHROPIC_VERSION, "content-type": "application/json"}
    if os.environ.get("ANTHROPIC_WORKSPACE_ID"):
        headers["anthropic-workspace-id"] = os.environ["ANTHROPIC_WORKSPACE_ID"]
    root = base_url.rstrip("/").removesuffix("/v1")
    resp = requests.post(f"{root}/v1/messages", timeout=timeout, headers=headers, json={
        "model": body["model"], "max_tokens": 4096, "temperature": 0,
        "messages": [{"role": "user", "content": content}]})
    resp.raise_for_status()
    r = resp.json()
    text = "".join(b.get("text", "") for b in r.get("content", []) if b.get("type") == "text")
    u = r.get("usage") or {}
    return {"choices": [{"message": {"content": _json_text(text)}}],
            "usage": {"prompt_tokens": u.get("input_tokens") or 0, "completion_tokens": u.get("output_tokens") or 0}}


def chat(base_url: str, api_key: str, body: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    if "ollama.com" in base_url:
        return _ollama(base_url, api_key, body, timeout)
    if str(body.get("model", "")).startswith("claude-"):
        return _anthropic(base_url, api_key, body, timeout)
    import requests

    resp = requests.post(f"{base_url.rstrip('/')}/chat/completions", json=body, timeout=timeout,
                         headers={"Authorization": f"Bearer {api_key}"})
    resp.raise_for_status()
    return resp.json()

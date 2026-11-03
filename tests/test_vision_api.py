"""vision_api.chat routes one OpenAI-shaped body to ollama native, Anthropic or /chat/completions."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests  # noqa: E402

import vision_api  # noqa: E402

BODY = {"model": "m", "messages": [{"role": "user", "content": [
    {"type": "text", "text": "sheet 1"}, {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,AAA"}},
    {"type": "text", "text": "answer"}]}]}


class Resp:
    def __init__(self, data):
        self.data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


def capture(monkeypatch, reply):
    sent = {}

    def post(url, json=None, timeout=None, headers=None):
        sent.update(url=url, body=json, headers=headers)
        return Resp(reply)
    monkeypatch.setattr(requests, "post", post)
    return sent


def test_ollama_native_low_think(monkeypatch):
    monkeypatch.delenv("SCOREBUG_VISION_THINK", raising=False)
    sent = capture(monkeypatch, {"message": {"content": '```json\n{"a": 1}\n```'}, "eval_count": 5})
    out = vision_api.chat("https://ollama.com/v1", "k", BODY, 10)
    assert sent["url"] == "https://ollama.com/api/chat" and sent["body"]["think"] == "low"
    assert sent["body"]["messages"] == [{"role": "user", "content": "sheet 1", "images": ["AAA"]},
                                        {"role": "user", "content": "answer", "images": []}]
    assert out["choices"][0]["message"]["content"] == '{"a": 1}' and out["usage"]["completion_tokens"] == 5


def test_claude_goes_to_messages_api(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_WORKSPACE_ID", "wrkspc_x")
    sent = capture(monkeypatch, {"content": [{"type": "text", "text": '{"a": 2}'}], "usage": {"input_tokens": 9}})
    out = vision_api.chat("https://api.anthropic.com", "k", dict(BODY, model="claude-sonnet-5-5"), 10)
    assert sent["url"] == "https://api.anthropic.com/v1/messages"
    assert sent["headers"]["anthropic-workspace-id"] == "wrkspc_x"
    assert sent["body"]["messages"][0]["content"][1]["source"]["data"] == "AAA"
    assert out["choices"][0]["message"]["content"] == '{"a": 2}' and out["usage"]["prompt_tokens"] == 9


def test_other_endpoints_unchanged(monkeypatch):
    sent = capture(monkeypatch, {"choices": [{"message": {"content": "{}"}}]})
    vision_api.chat("https://example.test/v1/", "k", BODY, 10)
    assert sent["url"] == "https://example.test/v1/chat/completions" and sent["body"] is BODY

"""Expose the local BGE model through an OpenAI-compatible embeddings endpoint.

Start with the Python environment that already has sentence-transformers:
    python tools/local_embedding_server.py

The Java LangChain4j application calls POST /v1/embeddings during RAG indexing
and retrieval. The model remains outside the Java project and is loaded locally.
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


MODEL_PATH = Path(
    os.environ.get(
        "AI_EMBEDDING_MODEL_PATH",
        r"D:\A_Stu\VSCode_WP\_7_Shix\_1_agent\all-in-rag\models\bge-small-zh-v1.5",
    )
)
HOST = os.environ.get("AI_EMBEDDING_HOST", "127.0.0.1")
PORT = int(os.environ.get("AI_EMBEDDING_PORT", "8088"))


def load_model():
    from sentence_transformers import SentenceTransformer

    if not MODEL_PATH.is_dir():
        raise FileNotFoundError(f"Embedding model directory does not exist: {MODEL_PATH}")
    return SentenceTransformer(str(MODEL_PATH), device=os.environ.get("AI_EMBEDDING_DEVICE", "cpu"))


MODEL = load_model()


class Handler(BaseHTTPRequestHandler):
    server_version = "HmdpEmbeddingServer/1.0"

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/", "/health", "/v1/health"}:
            self._send_json(200, {"status": "ok", "model": str(MODEL_PATH)})
            return
        self._send_json(404, {"error": {"message": "not found", "type": "invalid_request_error"}})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/embeddings":
            self._send_json(404, {"error": {"message": "not found", "type": "invalid_request_error"}})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length).decode("utf-8"))
            raw_input = request.get("input", "")
            texts = raw_input if isinstance(raw_input, list) else [raw_input]
            if not texts or any(not isinstance(text, str) for text in texts):
                raise ValueError("input must be a string or a list of strings")

            vectors = MODEL.encode(
                texts,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
            data = [
                {"object": "embedding", "embedding": vector.tolist(), "index": index}
                for index, vector in enumerate(vectors)
            ]
            self._send_json(
                200,
                {
                    "object": "list",
                    "data": data,
                    "model": request.get("model", "bge-small-zh-v1.5"),
                    "usage": {"prompt_tokens": 0, "total_tokens": 0},
                },
            )
        except Exception as exc:  # return an OpenAI-shaped error to the Java client
            self._send_json(400, {"error": {"message": str(exc), "type": "invalid_request_error"}})

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[embedding] {self.address_string()} - {format % args}", flush=True)


if __name__ == "__main__":
    print(f"[embedding] model={MODEL_PATH}", flush=True)
    print(f"[embedding] listening on http://{HOST}:{PORT}/v1/embeddings", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()

#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass
class EmbeddingResponse:
    vectors: list[list[float]]
    dimensions: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate OpenAI embeddings fixture for dataset vector-search tests."
    )
    parser.add_argument(
        "--input",
        default="vector_embeddings_input.txt",
        help="Path to input text file (default: vector_embeddings_input.txt)",
    )
    parser.add_argument(
        "--output",
        default="vector_embeddings.json",
        help="Path to output JSON fixture (default: vector_embeddings.json)",
    )
    parser.add_argument(
        "--model",
        default="text-embedding-3-small",
        help="OpenAI embedding model (default: text-embedding-3-small)",
    )
    parser.add_argument(
        "--dimensions",
        type=int,
        default=1536,
        help="Embedding dimensions (default: 1536)",
    )
    parser.add_argument(
        "--api-key-env",
        default="OPENAI_API_KEY",
        help="Environment variable containing API key (default: OPENAI_API_KEY)",
    )
    parser.add_argument(
        "--base-url",
        default="https://api.openai.com",
        help="OpenAI API base URL (default: https://api.openai.com)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="Batch size per API request (default: 64)",
    )
    parser.add_argument(
        "--retry-count",
        type=int,
        default=3,
        help="Retry count for transient API failures (default: 3)",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=1.5,
        help="Base retry delay in seconds (default: 1.5)",
    )
    return parser.parse_args()


def read_texts(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]

    texts = []
    seen = set()
    for line in lines:
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if text in seen:
            continue
        seen.add(text)
        texts.append(text)

    if not texts:
        raise ValueError("Input file has no texts after filtering comments/blank lines")

    return texts


def request_embeddings(
    *,
    base_url: str,
    api_key: str,
    model: str,
    dimensions: int,
    inputs: list[str],
) -> EmbeddingResponse:
    payload: dict[str, Any] = {
        "model": model,
        "input": inputs,
    }

    if dimensions > 0:
        payload["dimensions"] = dimensions

    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url=f"{base_url.rstrip('/')}/v1/embeddings",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )

    with urllib.request.urlopen(request, timeout=120) as response:
        response_body = response.read().decode("utf-8")

    parsed = json.loads(response_body)
    data = parsed.get("data")
    if not isinstance(data, list):
        raise RuntimeError("OpenAI response has no 'data' array")

    vectors: list[list[float]] = []
    for item in data:
        embedding = item.get("embedding") if isinstance(item, dict) else None
        if not isinstance(embedding, list):
            raise RuntimeError("OpenAI response item has invalid embedding")
        vectors.append([float(v) for v in embedding])

    if not vectors:
        raise RuntimeError("OpenAI response returned no embeddings")

    inferred_dimensions = len(vectors[0])
    for vector in vectors:
        if len(vector) != inferred_dimensions:
            raise RuntimeError("OpenAI response contains inconsistent embedding dimensions")

    return EmbeddingResponse(vectors=vectors, dimensions=inferred_dimensions)


def batched(items: list[str], batch_size: int) -> list[list[str]]:
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def input_sha256(texts: list[str]) -> str:
    payload = "\n".join(texts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def main() -> int:
    args = parse_args()

    api_key = os.getenv(args.api_key_env)
    if not api_key:
        print(
            f"Environment variable {args.api_key_env} is not set",
            file=sys.stderr,
        )
        return 2

    texts = read_texts(args.input)

    all_vectors: list[list[float]] = []
    output_dimensions: int | None = None

    for batch in batched(texts, args.batch_size):
        for attempt in range(args.retry_count + 1):
            try:
                response = request_embeddings(
                    base_url=args.base_url,
                    api_key=api_key,
                    model=args.model,
                    dimensions=args.dimensions,
                    inputs=batch,
                )
                all_vectors.extend(response.vectors)
                if output_dimensions is None:
                    output_dimensions = response.dimensions
                elif output_dimensions != response.dimensions:
                    raise RuntimeError("Embedding dimensions changed across batches")
                break
            except urllib.error.HTTPError as e:
                is_last = attempt == args.retry_count
                if is_last:
                    details = e.read().decode("utf-8", errors="replace")
                    print(
                        f"OpenAI HTTP error {e.code}: {details}",
                        file=sys.stderr,
                    )
                    return 1
                sleep_seconds = args.retry_delay_seconds * (2**attempt)
                time.sleep(sleep_seconds)
            except (urllib.error.URLError, TimeoutError) as e:
                is_last = attempt == args.retry_count
                if is_last:
                    print(f"Network error while calling OpenAI: {e}", file=sys.stderr)
                    return 1
                sleep_seconds = args.retry_delay_seconds * (2**attempt)
                time.sleep(sleep_seconds)

    if len(all_vectors) != len(texts):
        print(
            "Mismatch between input text count and returned vector count",
            file=sys.stderr,
        )
        return 1

    assert output_dimensions is not None

    fixture = {
        "provider": "openai",
        "model": args.model,
        "dimensions": output_dimensions,
        "input_sha256": input_sha256(texts),
        "vectors": {text: vector for text, vector in zip(texts, all_vectors, strict=True)},
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(fixture, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(
        f"Wrote {len(texts)} embeddings ({output_dimensions} dims) to {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

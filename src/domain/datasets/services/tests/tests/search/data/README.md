# Vector search test data

This folder contains reproducible embedding fixtures for
`test_dataset_search_vector.rs`.

## Files

- `vector_embeddings_input.txt`: one input text per line (blank lines and `#` comments are ignored)
- `generate_openai_embeddings.py`: regenerates embeddings via OpenAI API
- `vector_embeddings.json`: generated fixture loaded by tests

## Regenerate embeddings

```bash
cd src/domain/datasets/services/tests/tests/search/data
OPENAI_API_KEY=... python3 generate_openai_embeddings.py
```

Optional arguments:

```bash
python3 generate_openai_embeddings.py \
  --input vector_embeddings_input.txt \
  --output vector_embeddings.json \
  --model text-embedding-3-small \
  --dimensions 1536
```

Output schema (`vector_embeddings.json`):

```json
{
  "provider": "openai",
  "model": "text-embedding-3-small",
  "dimensions": 1536,
  "input_sha256": "...",
  "vectors": {
    "<original input text>": [0.0, 0.1, ...]
  }
}
```

Tests require every text used in vector-search scenarios to exist in `vectors`.

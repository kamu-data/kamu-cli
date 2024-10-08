# Examples of Verifiable Batch Queries
Query proofs allow you to hold any ODF node forever accountable for the result it provided you, no matter how much time had passed.

See [batch query commitments documentation](https://docs.kamu.dev/node/commitments/) and [REST API reference](https://docs.kamu.dev/node/protocols/rest-api/) for the overview of this mechanism.

The included script illustrates:
- how to query data and receive a cryptographic proof
- how to validate commitment consistency on the client side
- and how to ask another node to verify the commitment by reproducing the query.

Running:
```sh
pip install -r requirements.in
python ./example.py --node-url https://node.example.com --private-key <multibase-encoded-bytes>
```

Private key is used to show examples of failed verification (by forging a signature). If you don't provide it - those cases will be skipped.

#!/usr/bin/env python
import argparse
import copy
import canonicaljson
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
import hashlib
import requests
import json
import base64
import base58


def decode_multibase(multibase):
    prefix, value = multibase[0], multibase[1:]
    if prefix == 'u':
        b64_urlsafe_nopad = value
        b64_urlsafe = b64_urlsafe_nopad + "=" * ((4 - len(b64_urlsafe_nopad) % 4) % 4)
        return base64.urlsafe_b64decode(b64_urlsafe)
    elif prefix == 'f':
        return bytes.fromhex(value)
    elif prefix == 'z':
        return base58.b58decode(value)
    else:
        raise("Malformed multibase value")


def encode_multibase_base64_urlsafe_nopad(bin):
    return 'u' + base64.urlsafe_b64encode(bin).decode('utf-8').rstrip("=")


def multihash_sha3_256_multibase_base16(bin):
    return 'f1620' + hashlib.sha3_256(bin).hexdigest()


def public_key_from_did(did):
    assert did.startswith('did:key:')
    multicodec = decode_multibase(did[len('did:key:'):])
    assert multicodec[0:1].hex() == 'ed' # 0xed is multicodec value for Ed25519Pub
    return Ed25519PublicKey.from_public_bytes(multicodec[2:])

def main(args):
    # Query data
    resp = requests.get(
        args.node_url + "/query",
        params=dict(
            query=args.query,
            include="proof",
        )
    )
    resp.raise_for_status()
    resp_data = resp.json()

    print(">>> Node's response:")
    print(json.dumps(resp_data, indent=2))
    print()
    print()


    # Verify commitment consistency
    # This should always be done by the client after receiving a proof to ensure its disputable
    assert resp_data["commitment"]["inputHash"] == multihash_sha3_256_multibase_base16(
        canonicaljson.encode_canonical_json(resp_data["input"])
    )
    assert resp_data["commitment"]["outputHash"] == multihash_sha3_256_multibase_base16(
        canonicaljson.encode_canonical_json(resp_data["output"])
    )
    assert resp_data["commitment"]["subQueriesHash"] == multihash_sha3_256_multibase_base16(
        canonicaljson.encode_canonical_json(resp_data["subQueries"])
    )

    signature = decode_multibase(resp_data["proof"]["proofValue"])
    public_key = public_key_from_did(resp_data["proof"]["verificationMethod"])
    public_key.verify(signature, canonicaljson.encode_canonical_json(resp_data["commitment"]))
    print("Commitment is consistent!")


    print(">>> Commitment:")
    commitment = resp_data.copy()
    del commitment["output"]
    print(json.dumps(commitment, indent=2))
    print()
    print()


    # Remote validation through reproducibility
    print(">>> Verifying original commitment:")
    resp = requests.post(
        args.node_url + "/verify",
        json=commitment
    )
    resp.raise_for_status()
    resp_data = resp.json()
    print(json.dumps(resp_data, indent=2))
    print()
    print()
    assert resp_data["ok"] == True


    # Invalid request: input hash
    print(">>> Simulating invalid request (input hash):")
    invalid_commitment = copy.deepcopy(commitment)
    invalid_commitment["commitment"]["inputHash"] = "f1620bd01de1b46f8afe08e128ddd225acdb4457c09919d7c50c2054859a178de51a6"
    print(json.dumps(invalid_commitment, indent=2))
    print()
    print()

    print("Verification result:")
    resp = requests.post(
        args.node_url + "/verify",
        json=invalid_commitment
    )
    resp_data = resp.json()
    print(json.dumps(resp_data, indent=2))
    print()
    print()
    assert resp_data["ok"] == False
    assert resp_data["error"]["kind"] == "InvalidRequest::InputHash"


    # Invalid request: subQueries hash
    print(">>> Simulating invalid request (subQueries hash):")
    invalid_commitment = copy.deepcopy(commitment)
    invalid_commitment["commitment"]["subQueriesHash"] = "f1620bd01de1b46f8afe08e128ddd225acdb4457c09919d7c50c2054859a178de51a6"
    print(json.dumps(invalid_commitment, indent=2))
    print()
    print()

    print("Verification result:")
    resp = requests.post(
        args.node_url + "/verify",
        json=invalid_commitment
    )
    resp_data = resp.json()
    print(json.dumps(resp_data, indent=2))
    print()
    print()
    assert resp_data["ok"] == False
    assert resp_data["error"]["kind"] == "InvalidRequest::SubQueriesHash"


    # Invalid request: bad signature
    print(">>> Simulating invalid request (bad signature):")
    invalid_commitment = copy.deepcopy(commitment)
    invalid_commitment["proof"]["proofValue"] = "uZbm7fFcWc4l6iyvaKe_txdKntL3h3kvsGHOaKIbPV6c42PH1VnSmpYHMopv4TU68syzgoEdcS26AvpkSQb9dBQ"
    print(json.dumps(invalid_commitment, indent=2))
    print()
    print()

    print("Verification result:")
    resp = requests.post(
        args.node_url + "/verify",
        json=invalid_commitment
    )
    resp_data = resp.json()
    print(json.dumps(resp_data, indent=2))
    print()
    print()
    assert resp_data["ok"] == False
    assert resp_data["error"]["kind"] == "InvalidRequest::BadSignature"


    if args.private_key is None:
        print("Private key is not provided - skipping tests that require signature forging")
        return

    private_key = Ed25519PrivateKey.from_private_bytes(decode_multibase(args.private_key))


    # Cannot reproduce the query: output mismatch
    # Dataset stays the same but we fake the output hash and the signature
    print(">>> Simulating invalid request (output mismatch):")
    invalid_commitment = copy.deepcopy(commitment)
    invalid_commitment["commitment"]["outputHash"] = "f1620ff7f5beaf16900218a3ac4aae82cdccf764816986c7c739c716cf7dc03112a2d"

    canonical_commitment = canonicaljson.encode_canonical_json(invalid_commitment["commitment"])
    signature = private_key.sign(canonical_commitment)
    invalid_commitment["proof"]["proofValue"] = encode_multibase_base64_urlsafe_nopad(signature)

    print(json.dumps(invalid_commitment, indent=2))
    print()
    print()

    print("Verification result:")
    resp = requests.post(
        args.node_url + "/verify",
        json=invalid_commitment
    )
    resp_data = resp.json()
    print(json.dumps(resp_data, indent=2))
    print()
    print()
    assert resp_data["ok"] == False
    assert resp_data["error"]["kind"] == "VerificationFailed::OutputMismatch"


    # Cannot reproduce the query: dataset is missing
    # Dataset stays the same but we fake the output hash and the signature
    print(">>> Simulating invalid request (dataset is missing):")
    invalid_commitment = copy.deepcopy(commitment)
    invalid_commitment["input"]["datasets"][0]["id"] = invalid_commitment["input"]["datasets"][0]["id"][:-4] + "beef"
    invalid_commitment["commitment"]["inputHash"] = multihash_sha3_256_multibase_base16(
        canonicaljson.encode_canonical_json(
            invalid_commitment["input"]
        )
    )

    canonical_commitment = canonicaljson.encode_canonical_json(invalid_commitment["commitment"])
    signature = private_key.sign(canonical_commitment)
    invalid_commitment["proof"]["proofValue"] = encode_multibase_base64_urlsafe_nopad(signature)

    print(json.dumps(invalid_commitment, indent=2))
    print()
    print()

    print("Verification result:")
    resp = requests.post(
        args.node_url + "/verify",
        json=invalid_commitment
    )
    resp_data = resp.json()
    print(json.dumps(resp_data, indent=2))
    print()
    print()
    assert resp_data["ok"] == False
    assert resp_data["error"]["kind"] == "VerificationFailed::DatasetNotFound"


    # Cannot reproduce the query: block is missing
    # Dataset stays the same but we fake the output hash and the signature
    print(">>> Simulating invalid request (block is missing):")
    invalid_commitment = copy.deepcopy(commitment)
    invalid_commitment["input"]["datasets"][0]["blockHash"] = invalid_commitment["input"]["datasets"][0]["blockHash"][:-4] + "beef"
    invalid_commitment["commitment"]["inputHash"] = multihash_sha3_256_multibase_base16(
        canonicaljson.encode_canonical_json(
            invalid_commitment["input"]
        )
    )

    canonical_commitment = canonicaljson.encode_canonical_json(invalid_commitment["commitment"])
    signature = private_key.sign(canonical_commitment)
    invalid_commitment["proof"]["proofValue"] = encode_multibase_base64_urlsafe_nopad(signature)

    print(json.dumps(invalid_commitment, indent=2))
    print()
    print()

    print("Verification result:")
    resp = requests.post(
        args.node_url + "/verify",
        json=invalid_commitment
    )
    resp_data = resp.json()
    print(json.dumps(resp_data, indent=2))
    print()
    print()
    assert resp_data["ok"] == False
    assert resp_data["error"]["kind"] == "VerificationFailed::DatasetBlockNotFound"
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--node-url', required=True)
    parser.add_argument('--private-key', required=False)
    parser.add_argument('--query', default='select block_hash, to from "kamu/net.rocketpool.reth.tokens-minted" order by offset desc limit 1')
    args = parser.parse_args()
    main(args)
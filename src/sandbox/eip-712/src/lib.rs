use std::collections::{BTreeMap, HashMap};

use alloy::dyn_abi::{Eip712Domain, PropertyDef, TypedData};
use alloy::primitives::U256;
use serde::Deserialize;

/*

https://eips.ethereum.org/EIPS/eip-712#specification-of-the-eth_signtypeddata-json-rpc
{
  type: 'object',
  properties: {
    types: {
      type: 'object',
      properties: {
        EIP712Domain: {type: 'array'},
      },
      additionalProperties: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            name: {type: 'string'},
            type: {type: 'string'}
          },
          required: ['name', 'type']
        }
      },
      required: ['EIP712Domain']
    },
    primaryType: {type: 'string'},
    domain: {type: 'object'},
    message: {type: 'object'}
  },
  required: ['types', 'primaryType', 'domain', 'message']
}

POST /sign/eip712?key={did}&includeNodeProof=true
{
  "domain": {
    "name": "MoleculeOclDidRegistry",
    "version": "1",
    "chainId": 1, // ethereum mainnet or sepolia chainid
    "verifyingContract": "0x1234..5678" // MoleculeOclDidRegistry contract
address   },
  "types": {
    "LinkDidRequest": [ .. ]
  },
  "primaryType": "LinkDidRequest",
  "message": {
    "requestId": ...,
    "deadline": ...,
  }
}

struct LinkDidRequest {
    bytes32 oclId,
    bytes32 provider,
    bytes32 subject,
    string  did,
    bytes32 requestId,
    uint256 deadline,
}

*/

#[derive(Deserialize)]
struct DomainInput {
    name: String,
    version: String,
    chain_id: u64,
    verifying_contract: Option<String>,
}

#[derive(Deserialize)]
struct SignRequest {
    domain: DomainInput,
    primary_type: String,
    types: HashMap<String, Vec<FieldInput>>,
    message: serde_json::Map<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct FieldInput {
    name: String,
    r#type: String, // "address", "uint256", "string", etc.
}

impl FieldInput {
    pub fn new(name: impl Into<String>, r#type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            r#type: r#type.into(),
        }
    }
}

// TODO: better naming
fn to_typed_data(req: SignRequest) -> eyre::Result<TypedData> {
    let domain = Eip712Domain {
        name: Some(req.domain.name.into()),
        version: Some(req.domain.version.into()),
        chain_id: Some(U256::from(req.domain.chain_id)),
        // TODO: API verif? 20 chars?
        verifying_contract: req
            .domain
            .verifying_contract
            .map(|s| s.parse())
            .transpose()?,
        // TODO: do we need salt?
        salt: None,
    };

    let types: BTreeMap<String, Vec<PropertyDef>> = req
        .types
        .into_iter()
        .map(|(type_name, fields)| {
            let props: eyre::Result<Vec<PropertyDef>> = fields
                .into_iter()
                .map(|f| PropertyDef::new(f.r#type, f.name).map_err(|e| eyre::eyre!(e)))
                .collect();
            Ok((type_name, props?))
        })
        .collect::<eyre::Result<_>>()?;

    let resolver = serde_json::from_value(serde_json::to_value(&types)?)?;

    Ok(TypedData {
        domain,
        resolver,
        primary_type: req.primary_type,
        message: serde_json::Value::Object(req.message),
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy::primitives::{b256, keccak256};
    use alloy::signers::k256::ecdsa::SigningKey;
    use alloy::signers::local::LocalSigner;
    use alloy::signers::{Signature, SignerSync};
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_typed_data_signed_correctly() -> eyre::Result<()> {
        // Adapted from:
        // https://github.com/moleculeprotocol/onchainlabs/blob/main/docs/identity/kamu-eip712-linkdidrequest-handoff.md

        const DID: &str = "did:odf:ed25519:z6MkhaXgBZDvotAccount";

        let req = SignRequest {
            domain: DomainInput {
                name: "MoleculeOclDidRegistry".to_string(),
                version: "1".to_string(),
                chain_id: 8453,
                // Molecule's note: synthetic — see note in fixtures doc §2
                verifying_contract: Some("0x00000000000000000000000000000000DeaDBeeF".to_string()),
            },
            primary_type: "LinkDidRequest".to_string(),
            types: HashMap::from([(
                "LinkDidRequest".to_string(),
                vec![
                    FieldInput::new("oclId", "bytes32"),
                    FieldInput::new("provider", "bytes32"),
                    FieldInput::new("subject", "bytes32"),
                    FieldInput::new("did", "string"),
                    FieldInput::new("requestId", "bytes32"),
                    FieldInput::new("deadline", "uint256"),
                ],
            )]),
            message: {
                let json_obj = json!({
                    "oclId": "0x0101000000000000000000000000000000000000000000000000000000000042",
                    // keccak256("odf")
                    "provider": "0xd025eab60676ea26fe9a9a945c991e6d0f3f06e3a940bb5123899345a9b2a413",
                    // keccak256("account")
                    "subject": "0xd844bb55167ab332117049e2ccd3d8863d241bcc80f46302310a6d942a90e851",
                    "did": DID,
                    "requestId": "0xad68a4a8b76681274554d59f863c35d7abcef09a798c99e9717a2582037764a5",
                    // 2030-01-01T00:00:00Z
                    "deadline": "1893456000",
                });
                let serde_json::Value::Object(json_map) = json_obj else {
                    unreachable!();
                };
                json_map
            },
        };
        let typed_data = to_typed_data(req)?;

        // 4.1) keccak256(bytes(did))
        {
            let expected_hash =
                b256!("0xb6ed9810fd59a66adb2e17dd7f6142732ea24fd24c9a805d7b5d1eb606750a32");
            let actual_hash = keccak256(DID);

            assert_eq!(expected_hash, actual_hash);
        }

        // 4.2) Struct hash
        {
            let expected_hash = Ok(b256!(
                "0x1150be8b733f893e4d086fd0a993433dd43c8d1d056e5a1539221b359dbbf041"
            ));
            let actual_hash = typed_data.hash_struct();

            assert_eq!(expected_hash, actual_hash);
        }

        // 4.3) Domain separator
        {
            let expected_hash =
                b256!("0x9d056bc714b1d8ad38256fe8ba99f3e343766ceff53a0e9eb8c31b93295d1e7e");
            let actual_hash = typed_data.domain.separator();

            assert_eq!(expected_hash, actual_hash);
        }

        // 4.4) EIP-712 digest
        {
            let expected_signing_hash = Ok(b256!(
                "0xfc67633d930b129099d4600295c3676b35c191d90dd4b59274fb2dfd70396c9c"
            ));
            let actual_signing_hash = typed_data.eip712_signing_hash();

            assert_eq!(expected_signing_hash, actual_signing_hash);
        }

        // 5) Test private key + expected signature
        {
            let signer: LocalSigner<SigningKey> =
                "0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601".parse()?;
            let expected_signature = Signature::from_str(
                "0xf3073c2f0a3512fc896cb98d9a93c014f25c1dd758dd2c8e27d31aa6d6d2bc4756b739978bf784b52718653a46b9283b10f47359fee686bde8b70882e305eadc1c",
            )?;
            let actual_signature = signer.sign_dynamic_typed_data_sync(&typed_data)?;

            assert_eq!(expected_signature.to_string(), actual_signature.to_string());
        }

        Ok(())
    }
}

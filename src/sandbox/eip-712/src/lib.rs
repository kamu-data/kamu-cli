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
    use alloy::primitives::b256;
    use alloy::signers::k256::ecdsa::SigningKey;
    use alloy::signers::local::LocalSigner;
    use alloy::signers::{Signature, SignerSync};
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_typed_data_signed_correctly() -> eyre::Result<()> {
        let req = SignRequest {
            domain: DomainInput {
                name: "MoleculeOclDidRegistry".to_string(),
                version: "1".to_string(),
                chain_id: 1,
                verifying_contract: Some("0x1234567890123456789012345678901234567890".to_string()),
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
                    "oclId": "0x1234567890123456789012345678901234567890",
                    "provider": "0x1234567890123456789012345678901234567890",
                    "subject": "0x1234567890123456789012345678901234567890",
                    "did": "did:molecule:0x1234567890123456789012345678901234567890",
                    "requestId": "0x1234567890123456789012345678901234567890",
                    "deadline": "1000000000000000000",
                });
                let serde_json::Value::Object(json_map) = json_obj else {
                    unreachable!();
                };
                json_map
            },
        };
        let typed_data = to_typed_data(req)?;

        {
            let expected_hash = Ok(b256!(
                "0x6e366a386b5a566cd7252056fab0dcee73fa85a28b6b4be48910acbaeff104f8"
            ));
            let actual_hash = typed_data.hash_struct();

            assert_eq!(expected_hash, actual_hash);
        }

        {
            let signer: LocalSigner<SigningKey> =
                "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
            let expected_signature = Signature::new(
                "3947742182129489675771646903630550663162458215065625123408019227496657409676"
                    .parse()?,
                "14123466426607808506833502959344684099943903139190352731783630576333407444976"
                    .parse()?,
                true,
            );
            let actual_signature = signer.sign_dynamic_typed_data_sync(&typed_data)?;

            assert_eq!(expected_signature, actual_signature);
        }

        Ok(())
    }
}

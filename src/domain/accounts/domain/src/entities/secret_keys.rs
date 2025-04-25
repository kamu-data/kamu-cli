// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use internal_error::InternalError;
use secrecy::SecretString;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AccountSecretKey {
    pub secret_key: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

impl AccountSecretKey {
    pub fn new(secret_key: SecretString, encryption_key: &str,) -> Result<Self, InternalError> {
        unimplemented!()

        // let dataset_env_var_id = Uuid::new_v4();
        // let mut secret_nonce: Option<Vec<u8>> = None;
        // let final_value: Vec<u8>;

        // match dataset_env_var_value {
        //     DatasetEnvVarValue::Secret(secret_value) => {
        //         let cipher = Self::try_asm_256_gcm_from_str(encryption_key)?;
        //         let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        //         secret_nonce = Some(nonce.to_vec());
        //         final_value = cipher
        //             .encrypt(&nonce, secret_value.expose_secret().as_ref())
        //             .map_err(|err| DatasetEnvVarEncryptionError::InvalidCipherKeyError {
        //                 source: Box::new(AesGcmError(err)),
        //             })?;
        //     }
        //     DatasetEnvVarValue::Regular(value) => final_value = value.as_bytes().to_vec(),
        // }

        // Ok(DatasetEnvVar {
        //     id: dataset_env_var_id,
        //     value: final_value,
        //     secret_nonce,
        //     key: dataset_env_var_key.to_string(),
        //     created_at: creation_date,
        //     dataset_id: dataset_id.clone(),
        // })
    }

    // pub fn get_non_secret_value(&self) -> Option<String> {
    //     if self.secret_nonce.is_none() {
    //         return Some(std::str::from_utf8(&self.value).unwrap().to_string());
    //     }
    //     None
    // }

    // pub fn get_exposed_decrypted_value(
    //     &self,
    //     encryption_key: &str,
    // ) -> Result<String, DatasetEnvVarEncryptionError> {
    //     if let Some(secret_nonce) = self.secret_nonce.as_ref() {
    //         let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(encryption_key.as_bytes()));
    //         let decrypted_value = cipher
    //             .decrypt(
    //                 GenericArray::from_slice(secret_nonce.as_slice()),
    //                 self.value.as_ref(),
    //             )
    //             .map_err(|err| DatasetEnvVarEncryptionError::InvalidCipherKeyError {
    //                 source: Box::new(AesGcmError(err)),
    //             })?;
    //         return Ok(std::str::from_utf8(decrypted_value.as_slice())
    //             .map_err(|err| DatasetEnvVarEncryptionError::InternalError(err.int_err()))?
    //             .to_string());
    //     }
    //     Ok(std::str::from_utf8(&self.value).unwrap().to_string())
    // }
}

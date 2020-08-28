// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::vec::Vec;
use tikv_client::{Config, RawClient as Client};

pub struct DB {
    client: Client,
}

impl DB {
    pub async fn new(pd_endpoints: &str) -> Self {
        let endpoints: Vec<String> = pd_endpoints
            .split_terminator(',')
            .map(|s| s.to_string())
            .collect();
        let config = Config::new(endpoints);
        let client = Client::new(config).await.unwrap();
        DB { client }
    }

    pub async fn store(&self, region: u32, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        let mut kv_key = region.to_be_bytes().to_vec();
        kv_key.extend_from_slice(key.as_slice());
        let ret = self.client.put(kv_key, value).await;
        ret.map(|_| ()).map_err(|e| format!("store error: {:?}", e))
    }

    pub async fn load(&self, region: u32, key: Vec<u8>) -> Result<Vec<u8>, String> {
        let mut kv_key = region.to_be_bytes().to_vec();
        kv_key.extend_from_slice(key.as_slice());
        let ret = self.client.get(kv_key).await;
        ret.map(|opt_v| opt_v.unwrap_or_default())
            .map_err(|e| format!("load error: {:?}", e))
    }

    pub async fn delete(&self, region: u32, key: Vec<u8>) -> Result<(), String> {
        let mut kv_key = region.to_be_bytes().to_vec();
        kv_key.extend_from_slice(key.as_slice());
        let ret = self.client.delete(kv_key).await;
        ret.map(|_| ())
            .map_err(|e| format!("delete error: {:?}", e))
    }
}

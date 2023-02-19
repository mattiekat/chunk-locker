use std::io::Read;

use once_cell::sync::OnceCell;

use crate::memory::MemoryConfig;
use serde::Deserialize;

use crate::store::StoreConfig;

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default)]
    pub store: StoreConfig,
    #[serde(default)]
    pub memory: MemoryConfig,
}

static CONFIG: OnceCell<Config> = OnceCell::new();

impl Config {
    pub fn load() -> &'static Self {
        CONFIG.get_or_init(|| {
            let mut file = if cfg!(test) {
                std::fs::File::open("test/fixtures/config.toml")
                    .expect("Unable to find test config file")
            } else {
                std::fs::File::open("config.toml").expect("Unable to find config.toml file")
            };

            let mut config_str = String::new();
            file.read_to_string(&mut config_str)
                .expect("Could not read from file");
            toml::from_str(&config_str).expect("Unable to parse config file")
        })
    }
}

use std::io::Read;

use once_cell::sync::OnceCell;

use serde::Deserialize;

use crate::store::StoreConfig;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub store: StoreConfig,
}

static config: OnceCell<Config> = OnceCell::new();

impl Config {
    pub fn load() -> &'static Self {
        config.get_or_init(|| {
            let mut file =
                std::fs::File::open("config.toml").expect("Unable to find config.toml file");
            let mut config_str = String::new();
            file.read_to_string(&mut config_str).unwrap();
            toml::from_str(&config_str).expect("Unable to parse config file")
        })
    }
}

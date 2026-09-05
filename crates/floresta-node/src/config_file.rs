// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fs;
use std::path::Path;
use std::str::FromStr;

use bitcoin::Address;
use bitcoin::ScriptBuf;
use serde::Deserialize;
use tracing::debug;
use tracing::error;
use tracing::warn;

use crate::error::FlorestadError;
use crate::florestad::Config;

#[derive(Default, Debug, Deserialize)]
pub struct Wallet {
    pub xpubs: Option<Vec<String>>,
    pub descriptors: Option<Vec<String>>,
    pub addresses: Option<Vec<String>>,
}

#[derive(Default, Debug, Deserialize)]
pub struct ConfigFile {
    pub wallet: Wallet,
}

impl ConfigFile {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, FlorestadError> {
        let config_file = fs::read_to_string(path.as_ref())?;

        Ok(toml::from_str(&config_file)?)
    }
}

/// Load config from disk; prefer explicit `config_file`, otherwise use `{data_dir}/config.toml`.
/// Returns default if it cannot load it.
///
/// This should be called exactly once per boot, and the result shared with everyone that needs
/// it. Reading it more than once may observe different file contents on each read, and repeats
/// the warnings below.
pub fn load_config_file(config: &Config) -> ConfigFile {
    let path = match config.config_file.as_ref() {
        Some(path) => path.clone(),
        None => config.datadir.join("config.toml"),
    };

    match ConfigFile::from_file(&path) {
        Ok(data) => data,
        Err(FlorestadError::Io(e)) => {
            warn!("Could not read config file, ignoring it");
            debug!("{e}");
            ConfigFile::default()
        }
        Err(FlorestadError::TomlParsing(e)) => {
            warn!("Could not parse config file, ignoring it");
            debug!("{e}");
            ConfigFile::default()
        }
        // Shouldn't be any other error
        Err(_) => unreachable!(),
    }
}

/// The wallet inputs for this boot, resolved from every source we take them from.
///
/// Built once by [`WalletConfig::resolve`], then handed to the watch-only wallet setup.
pub struct WalletConfig {
    /// Output descriptors to add to the wallet. Validated by the wallet itself.
    pub descriptors: Vec<String>,

    /// SLIP-132 extended public keys to add to the wallet. Validated by the wallet itself.
    pub xpubs: Vec<String>,

    /// Addresses to watch, already parsed into their script pubkeys.
    pub addresses: Vec<ScriptBuf>,
}

impl WalletConfig {
    /// Merge the wallet settings from [`Config`], the config file and the environment.
    ///
    /// Vectors are concatenated, with [`Config`] (i.e. the command line) coming first, as
    /// documented in [`Config::config_file`]. `env_xpub` is the `WALLET_XPUB` environment
    /// variable, passed in so that this stays a pure function.
    ///
    /// Fails if an address can't be parsed.
    pub fn resolve(
        config: &Config,
        file: &ConfigFile,
        env_xpub: Option<String>,
    ) -> Result<Self, FlorestadError> {
        let descriptors = config
            .wallet_descriptor
            .iter()
            .flatten()
            .chain(file.wallet.descriptors.iter().flatten())
            .cloned()
            .collect();

        let xpubs = config
            .wallet_xpub
            .iter()
            .flatten()
            .chain(file.wallet.xpubs.iter().flatten())
            .chain(env_xpub.iter())
            .cloned()
            .collect();

        let addresses = file
            .wallet
            .addresses
            .iter()
            .flatten()
            .map(|addr_str| {
                Address::from_str(addr_str)
                    .map(|addr| addr.assume_checked().script_pubkey())
                    .map_err(|e| {
                        error!("Invalid address provided: {addr_str} \nReason: {e:?}");
                        FlorestadError::from(e)
                    })
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            descriptors,
            xpubs,
            addresses,
        })
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::Network;
    use pretty_assertions::assert_eq;

    use super::*;

    /// A [Config] with no wallet settings at all, on the given network.
    fn config(network: Network) -> Config {
        Config::new(network, "/tmp/floresta-config-tests")
    }

    /// A [ConfigFile] with the given wallet settings, `None` meaning "key absent from the file".
    fn config_file(
        descriptors: Option<&[&str]>,
        xpubs: Option<&[&str]>,
        addresses: Option<&[&str]>,
    ) -> ConfigFile {
        let owned = |v: Option<&[&str]>| {
            v.map(|items| items.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };

        ConfigFile {
            wallet: Wallet {
                descriptors: owned(descriptors),
                xpubs: owned(xpubs),
                addresses: owned(addresses),
            },
        }
    }

    #[test]
    fn resolve_without_any_wallet_settings_is_empty() {
        let resolved =
            WalletConfig::resolve(&config(Network::Bitcoin), &ConfigFile::default(), None).unwrap();

        assert!(resolved.descriptors.is_empty());
        assert!(resolved.xpubs.is_empty());
        assert!(resolved.addresses.is_empty());
    }

    #[test]
    fn resolve_concatenates_descriptors_with_config_first() {
        let mut config = config(Network::Bitcoin);
        config.wallet_descriptor = Some(vec!["from_config".into()]);
        let file = config_file(Some(&["from_file"]), None, None);

        let resolved = WalletConfig::resolve(&config, &file, None).unwrap();

        assert_eq!(resolved.descriptors, vec!["from_config", "from_file"]);
    }

    #[test]
    fn resolve_concatenates_xpubs_with_the_env_last() {
        let mut config = config(Network::Bitcoin);
        config.wallet_xpub = Some(vec!["from_config".into()]);
        let file = config_file(None, Some(&["from_file"]), None);

        let resolved = WalletConfig::resolve(&config, &file, Some("from_env".into())).unwrap();

        assert_eq!(resolved.xpubs, vec!["from_config", "from_file", "from_env"]);
    }

    #[test]
    fn resolve_parses_addresses_into_script_pubkeys() {
        let address = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2";
        let file = config_file(None, None, Some(&[address]));

        let resolved = WalletConfig::resolve(&config(Network::Bitcoin), &file, None).unwrap();

        let expected = Address::from_str(address)
            .unwrap()
            .assume_checked()
            .script_pubkey();
        assert_eq!(resolved.addresses, vec![expected]);
    }

    #[test]
    fn resolve_fails_on_an_unparseable_address() {
        let file = config_file(None, None, Some(&["not an address"]));

        let resolved = WalletConfig::resolve(&config(Network::Bitcoin), &file, None);

        assert!(matches!(resolved, Err(FlorestadError::AddressParsing(_))));
    }
}

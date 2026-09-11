// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::str::FromStr;

use bitcoin::Address;
use bitcoin::ScriptBuf;
use serde::Deserialize;
use tracing::info;

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
    /// Wallet settings. Absent from the file means the same as an empty table: no wallet
    /// settings. An empty file is a config that asks for nothing, not a malformed one.
    #[serde(default)]
    pub wallet: Wallet,
}

impl ConfigFile {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, FlorestadError> {
        let path = path.as_ref();

        let config_file = fs::read_to_string(path)
            .map_err(|e| FlorestadError::CouldNotReadConfigFile(path.to_path_buf(), e))?;

        toml::from_str(&config_file)
            .map_err(|e| FlorestadError::CouldNotParseConfigFile(path.to_path_buf(), e))
    }
}

/// Load config from disk; prefer explicit `config_file`, otherwise use `{data_dir}/config.toml`.
///
/// Running without a config file is supported: if we weren't given an explicit path and there is
/// nothing at the default one, we return the defaults. Anything else is fatal — if there is a
/// config file, or the user named one, we either honour it or refuse to start. Booting with a
/// half-understood config would silently give the node a different wallet than the operator asked
/// for.
///
/// This should be called exactly once per boot, and the result shared with everyone that needs
/// it. Reading it more than once may observe different file contents on each read.
pub fn load_config_file(config: &Config) -> Result<ConfigFile, FlorestadError> {
    let explicit = config.config_file.is_some();
    let path = match config.config_file.as_ref() {
        Some(path) => path.clone(),
        None => config.datadir.join("config.toml"),
    };

    match ConfigFile::from_file(&path) {
        Ok(file) => {
            info!("Starting florestad with config file at {}", path.display());
            Ok(file)
        }
        // Only the default path is allowed to be missing. A path we were explicitly given is an
        // instruction, and quietly ignoring a typo in it would start a node watching nothing.
        Err(FlorestadError::CouldNotReadConfigFile(_, e))
            if !explicit && e.kind() == ErrorKind::NotFound =>
        {
            info!("Starting florestad with defaults, no config file passed");
            Ok(ConfigFile::default())
        }
        Err(e) => Err(e),
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

    /// Addresses to watch, already parsed and checked against our network.
    pub addresses: Vec<ScriptBuf>,
}

impl WalletConfig {
    /// Merge the wallet settings from [`Config`], the config file and the environment.
    ///
    /// Vectors are concatenated, with [`Config`] (i.e. the command line) coming first, as
    /// documented in [`Config::config_file`]. `env_xpub` is the `WALLET_XPUB` environment
    /// variable, passed in so that this stays a pure function.
    ///
    /// Fails if an address can't be parsed, or isn't valid for `config.network`.
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
                    .and_then(|addr| addr.require_network(config.network))
                    .map(|addr| addr.script_pubkey())
                    .map_err(|e| FlorestadError::InvalidWalletAddress(addr_str.clone(), e))
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
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bitcoin::Network;
    use pretty_assertions::assert_eq;

    use super::*;

    /// A [Config] with no wallet settings at all, on the given network.
    fn config(network: Network) -> Config {
        Config::new(network, "/tmp/floresta-config-tests")
    }

    /// An empty data dir of our own, so tests don't step on each other's config files.
    ///
    /// Removed and recreated on each call, so a previous run leaving files behind can't make a
    /// later one pass or fail for the wrong reason.
    fn datadir() -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);

        let path = std::env::temp_dir().join(format!(
            "floresta-config-tests-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));

        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("can create a temp data dir");

        path
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
            .require_network(Network::Bitcoin)
            .unwrap()
            .script_pubkey();
        assert_eq!(resolved.addresses, vec![expected]);
    }

    #[test]
    fn resolve_fails_on_an_unparseable_address() {
        let file = config_file(None, None, Some(&["not an address"]));

        let resolved = WalletConfig::resolve(&config(Network::Bitcoin), &file, None);

        assert!(matches!(
            resolved,
            Err(FlorestadError::InvalidWalletAddress(addr, _)) if addr == "not an address"
        ));
    }

    #[test]
    fn resolve_fails_on_an_address_from_another_network() {
        // A mainnet address, given to a node running on signet
        let address = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2";
        let file = config_file(None, None, Some(&[address]));

        let resolved = WalletConfig::resolve(&config(Network::Signet), &file, None);

        let err = resolved
            .err()
            .expect("a mainnet address is not valid on signet");
        assert!(
            matches!(err, FlorestadError::InvalidWalletAddress(ref addr, _) if addr == address)
        );

        // Refusing to boot over this is only defensible if the message says what to fix, so
        // pin both halves of it.
        let message = err.to_string();
        assert!(message.contains(address), "{message}");
        assert!(message.contains("signet"), "{message}");
    }

    #[test]
    fn load_without_a_config_file_falls_back_to_defaults() {
        let config = Config::new(Network::Bitcoin, datadir());

        let file = load_config_file(&config).expect("a missing default config file is not fatal");

        assert!(file.wallet.xpubs.is_none());
        assert!(file.wallet.descriptors.is_none());
        assert!(file.wallet.addresses.is_none());
    }

    #[test]
    fn load_treats_a_config_file_without_wallet_settings_as_defaults() {
        // An empty file, a file that is all comments, and a file with an empty `[wallet]` table
        // are all configs that ask for no wallet, not malformed ones.
        for contents in ["", "# nothing to see here\n", "[wallet]\n"] {
            let datadir = datadir();
            fs::write(datadir.join("config.toml"), contents).unwrap();
            let config = Config::new(Network::Bitcoin, datadir);

            let file = load_config_file(&config)
                .unwrap_or_else(|e| panic!("{contents:?} should not be fatal, got: {e}"));

            assert!(file.wallet.xpubs.is_none());
            assert!(file.wallet.descriptors.is_none());
            assert!(file.wallet.addresses.is_none());
        }
    }

    #[test]
    fn load_reads_the_config_file_from_the_data_dir() {
        let datadir = datadir();
        fs::write(
            datadir.join("config.toml"),
            "[wallet]\nxpubs = [\"from_file\"]\n",
        )
        .unwrap();
        let config = Config::new(Network::Bitcoin, datadir);

        let file = load_config_file(&config).unwrap();

        assert_eq!(file.wallet.xpubs, Some(vec!["from_file".to_string()]));
    }

    #[test]
    fn load_fails_when_an_explicitly_passed_config_file_is_missing() {
        let datadir = datadir();
        let mut config = Config::new(Network::Bitcoin, &datadir);
        config.config_file = Some(datadir.join("does-not-exist.toml"));

        let loaded = load_config_file(&config);

        assert!(matches!(
            loaded,
            Err(FlorestadError::CouldNotReadConfigFile(..))
        ));
    }

    #[test]
    fn load_fails_on_a_malformed_config_file() {
        let datadir = datadir();
        fs::write(datadir.join("config.toml"), "this is not toml [[[").unwrap();
        let config = Config::new(Network::Bitcoin, datadir);

        let loaded = load_config_file(&config);

        assert!(matches!(
            loaded,
            Err(FlorestadError::CouldNotParseConfigFile(..))
        ));
    }
}

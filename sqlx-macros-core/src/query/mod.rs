use std::path::Path;

use proc_macro2::TokenStream;
use syn::Type;

pub use input::QueryMacroInput;
use quote::{format_ident, quote};
use sqlx_core::database::Database;
use sqlx_core::{column::Column, describe::Describe, type_info::TypeInfo};

use crate::database::DatabaseExt;
use crate::query::data::{hash_profiled_query, DynQueryData, QueryData};
use crate::query::input::RecordType;
use crate::query::metadata::MacrosEnv;
use either::Either;
use sqlx_core::config::Config;
use url::Url;

mod args;
mod cache;
mod data;
mod input;
mod metadata;
mod output;

#[derive(Copy, Clone)]
pub struct QueryDriver {
    db_name: &'static str,
    url_schemes: &'static [&'static str],
    expand:
        fn(&Config, QueryMacroInput, QueryDataSource, Option<&Path>) -> crate::Result<TokenStream>,
}

impl QueryDriver {
    pub const fn new<DB: DatabaseExt>() -> Self
    where
        Describe<DB>: serde::Serialize + serde::de::DeserializeOwned,
    {
        QueryDriver {
            db_name: DB::NAME,
            url_schemes: DB::URL_SCHEMES,
            expand: expand_with::<DB>,
        }
    }
}
pub enum QueryDataSource<'a> {
    Live {
        database_url: &'a str,
        database_url_parsed: Url,
    },
    Cached(DynQueryData),
}

impl<'a> QueryDataSource<'a> {
    pub fn live(database_url: &'a str) -> crate::Result<Self> {
        Ok(QueryDataSource::Live {
            database_url,
            database_url_parsed: database_url.parse()?,
        })
    }

    pub fn matches_driver(&self, driver: &QueryDriver) -> bool {
        match self {
            Self::Live {
                database_url_parsed,
                ..
            } => driver.url_schemes.contains(&database_url_parsed.scheme()),
            Self::Cached(dyn_data) => dyn_data.db_name == driver.db_name,
        }
    }
}
pub fn expand_input<'a>(
    input: QueryMacroInput,
    drivers: impl IntoIterator<Item = &'a QueryDriver>,
) -> crate::Result<TokenStream> {
    let metadata = metadata::try_for_crate()?;

    let metadata_env = metadata.env()?;
    let profile = input
        .profile
        .as_deref()
        .map(|name| {
            metadata
                .config
                .macros
                .profiles
                .get(name)
                .ok_or_else(|| format!("unknown query profile {name:?}"))
        })
        .transpose()?;
    let profile_url = if metadata_env.offline.unwrap_or(false) {
        None
    } else {
        profile
            .map(|profile| {
                metadata
                    .profile_url(&profile.database_url_var)?
                    .map(|url| suffix_url_query_value(&url, profile.url_suffix.as_deref()))
                    .transpose()
            })
            .transpose()?
            .flatten()
    };
    let offline_dir = profile
        .map(|profile| {
            metadata::configured_offline_dir(
                &metadata.workspace_root(),
                Some(&profile.offline_dir),
                None,
            )
        })
        .unwrap_or_else(|| metadata.offline_dir(&metadata_env));

    let data_source = match (&*metadata_env, profile_url.as_deref()) {
        (MacrosEnv { offline: None | Some(false), .. }, Some(url)) if profile.is_some() => {
            QueryDataSource::live(url)?
        }
        (MacrosEnv {
            offline: None | Some(false),
            database_url: Some(db_url),
            ..
        }, _)
        // Allow `DATABASE_URL=''`
        if profile.is_none() && !db_url.is_empty() => QueryDataSource::live(db_url)?,
        (MacrosEnv { offline, .. }, _) => {
            let filename = format!(
                "query-{}.json",
                hash_profiled_query(&input.sql, input.profile.as_deref())
            );
            let data_file_path = find_cached_query(
                &metadata.manifest_dir,
                &metadata.workspace_root(),
                offline_dir.as_deref(),
                profile.is_some() || metadata.config.common.offline_dir.is_some(),
                &filename,
            );

            let Some(data_file_path) = data_file_path else {
                return Err(
                    if offline.unwrap_or(false) {
                        "`SQLX_OFFLINE=true` but there is no cached data for this query, run `cargo sqlx prepare` to update the query cache or unset `SQLX_OFFLINE`"
                    } else {
                        "set `DATABASE_URL` to use query macros online, or run `cargo sqlx prepare` to update the query cache"
                    }.into()
                );
            };

            QueryDataSource::Cached(DynQueryData::from_data_file(
                &data_file_path,
                &input.sql,
                input.profile.as_deref(),
            )?)
        }
    };

    for driver in drivers {
        if data_source.matches_driver(driver) {
            let save_dir = match data_source {
                QueryDataSource::Live { .. } if profile.is_some() => offline_dir.as_deref(),
                QueryDataSource::Live { .. } if crate::env_opt("SQLX_TMP")?.is_some() => {
                    metadata_env.offline_dir.as_deref()
                }
                QueryDataSource::Live { .. } if metadata.config.common.offline_dir.is_some() => {
                    None
                }
                QueryDataSource::Live { .. } => offline_dir.as_deref(),
                QueryDataSource::Cached(_) => None,
            };
            return (driver.expand)(&metadata.config, input, data_source, save_dir);
        }
    }

    match data_source {
        QueryDataSource::Live {
            database_url_parsed,
            ..
        } => Err(format!(
            "no database driver found matching URL scheme {:?}; the corresponding Cargo feature may need to be enabled", 
            database_url_parsed.scheme()
        ).into()),
        QueryDataSource::Cached(data) => {
            Err(format!(
                "found cached data for database {:?} but no matching driver; the corresponding Cargo feature may need to be enabled",
                data.db_name
            ).into())
        }
    }
}

fn suffix_url_query_value(url: &str, suffix: Option<&str>) -> crate::Result<String> {
    let Some(suffix) = suffix else {
        return Ok(url.to_owned());
    };
    let mut parsed = Url::parse(url)?;
    let mut matches = 0;
    let pairs = parsed
        .query_pairs()
        .map(|(key, value)| {
            let value = if key == "connection_string" {
                matches += 1;
                format!("{value}{suffix}")
            } else {
                value.into_owned()
            };
            (key.into_owned(), value)
        })
        .collect::<Vec<_>>();
    if matches != 1 {
        return Err(
            "profile URL suffix requires exactly one connection_string query parameter".into(),
        );
    }
    parsed.query_pairs_mut().clear().extend_pairs(pairs);
    Ok(parsed.into())
}

fn find_cached_query(
    manifest_dir: &Path,
    workspace_root: &Path,
    offline_dir: Option<&Path>,
    isolated: bool,
    filename: &str,
) -> Option<std::path::PathBuf> {
    if isolated {
        return offline_dir
            .map(|dir| dir.join(filename))
            .filter(|path| path.exists());
    }

    [
        offline_dir.map(Path::to_path_buf),
        Some(manifest_dir.join(".sqlx")),
        Some(workspace_root.join(".sqlx")),
    ]
    .into_iter()
    .flatten()
    .map(|dir| dir.join(filename))
    .find(|path| path.exists())
}

#[cfg(test)]
mod offline_cache_tests {
    use super::{find_cached_query, hash_profiled_query, suffix_url_query_value};
    use std::fs;

    #[test]
    fn cached_query_rejects_wrong_role() {
        let root = std::env::temp_dir().join(format!("sqlx-profile-cache-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let query = "SELECT 1";
        let hash = hash_profiled_query(query, Some("write"));
        let path = root.join(format!("query-{hash}.json"));
        fs::write(
            &path,
            serde_json::json!({
                "db_name": "ODBC", "query": query, "describe": {}, "hash": hash, "profile": "write"
            })
            .to_string(),
        )
        .unwrap();
        assert!(super::DynQueryData::from_data_file(&path, query, None).is_err());
        assert!(super::DynQueryData::from_data_file(&path, query, Some("read")).is_err());
        assert!(super::DynQueryData::from_data_file(&path, query, Some("write")).is_ok());
        fs::write(
            &path,
            serde_json::json!({
                "db_name": "ODBC", "query": query, "describe": {},
                "hash": hash_profiled_query(query, None), "profile": "write"
            })
            .to_string(),
        )
        .unwrap();
        assert!(super::DynQueryData::from_data_file(&path, query, Some("write")).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn profiled_cache_keys_do_not_collide() {
        let query = "SELECT 1";
        assert_ne!(
            hash_profiled_query(query, None),
            hash_profiled_query(query, Some("write"))
        );
        assert_ne!(
            hash_profiled_query(query, Some("read")),
            hash_profiled_query(query, Some("write"))
        );
        assert_eq!(
            hash_profiled_query(query, None),
            super::data::hash_string(query)
        );
    }

    #[test]
    fn profile_suffix_encodes_decoded_query_value() {
        let url = suffix_url_query_value(
            "odbc://?connection_string=DSN%3Dtest%3BPWD%3D%7Ba%3Bb%7D&other=ok",
            Some(";ROLE=write"),
        )
        .unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(
            parsed
                .query_pairs()
                .find(|(key, _)| key == "connection_string")
                .unwrap()
                .1,
            "DSN=test;PWD={a;b};ROLE=write"
        );
        assert_eq!(
            parsed
                .query_pairs()
                .find(|(key, _)| key == "other")
                .unwrap()
                .1,
            "ok"
        );
        assert!(suffix_url_query_value("odbc://?other=x", Some(";ROLE=write")).is_err());
        assert!(suffix_url_query_value(
            "odbc://?connection_string=x&connection_string=y",
            Some(";ROLE=write")
        )
        .is_err());
        assert_eq!(
            suffix_url_query_value("odbc://?connection_string=x", None).unwrap(),
            "odbc://?connection_string=x"
        );
    }

    #[test]
    fn isolated_cache_never_reads_another_backend_or_workspace_fallback() {
        let root =
            std::env::temp_dir().join(format!("sqlx-cache-isolation-{}", std::process::id()));
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        let oe = root.join("oe");
        let pg = root.join("pg");
        let crate_dir = root.join("crate");
        fs::create_dir_all(&oe).unwrap();
        fs::create_dir_all(&pg).unwrap();
        fs::create_dir_all(crate_dir.join(".sqlx")).unwrap();
        fs::write(pg.join("query-shared.json"), "postgres").unwrap();
        fs::write(crate_dir.join(".sqlx/query-shared.json"), "fallback").unwrap();

        assert_eq!(
            find_cached_query(&crate_dir, &root, Some(&oe), true, "query-shared.json"),
            None
        );
        fs::write(oe.join("query-shared.json"), "odbc").unwrap();
        assert_eq!(
            find_cached_query(&crate_dir, &root, Some(&oe), true, "query-shared.json"),
            Some(oe.join("query-shared.json"))
        );
        assert_eq!(
            find_cached_query(&crate_dir, &root, Some(&pg), true, "query-shared.json"),
            Some(pg.join("query-shared.json"))
        );
        assert_eq!(
            find_cached_query(&crate_dir, &root, None, false, "query-shared.json"),
            Some(crate_dir.join(".sqlx/query-shared.json"))
        );
        fs::remove_dir_all(&root).unwrap();
    }
}

fn expand_with<DB: DatabaseExt>(
    config: &Config,
    input: QueryMacroInput,
    data_source: QueryDataSource,
    offline_dir: Option<&Path>,
) -> crate::Result<TokenStream>
where
    Describe<DB>: DescribeExt,
{
    let (query_data, save_dir): (QueryData<DB>, Option<&Path>) = match data_source {
        // If the build is offline, the cache is our input so it's pointless to also write data for it.
        QueryDataSource::Cached(dyn_data) => (QueryData::from_dyn_data(dyn_data)?, None),
        QueryDataSource::Live { database_url, .. } => {
            let describe = DB::describe_blocking(&input.sql, database_url, &config.drivers)?;
            let mut data = QueryData::from_describe(&input.sql, describe);
            data.profile = input.profile.clone();
            data.hash = hash_profiled_query(&input.sql, input.profile.as_deref());
            (data, offline_dir)
        }
    };

    expand_with_data(config, input, query_data, save_dir)
}

// marker trait for `Describe` that lets us conditionally require it to be `Serialize + Deserialize`
trait DescribeExt: serde::Serialize + serde::de::DeserializeOwned {}

impl<DB: Database> DescribeExt for Describe<DB> where
    Describe<DB>: serde::Serialize + serde::de::DeserializeOwned
{
}

#[derive(Default)]
struct Warnings {
    ambiguous_datetime: bool,
    ambiguous_numeric: bool,
}

fn expand_with_data<DB: DatabaseExt>(
    config: &Config,
    input: QueryMacroInput,
    data: QueryData<DB>,
    save_dir: Option<&Path>,
) -> crate::Result<TokenStream>
where
    Describe<DB>: DescribeExt,
{
    if DB::NAME == "ODBC" && !matches!(data.describe.parameters(), Some(Either::Left(_))) {
        return Err(
            "ODBC checked queries require parameter type metadata, including in offline mode"
                .into(),
        );
    }
    if input.profile != data.profile
        || data.hash != hash_profiled_query(&input.sql, input.profile.as_deref())
    {
        return Err("query metadata does not match its query profile".into());
    }

    // validate at the minimum that our args match the query's input parameters
    let num_parameters = match data.describe.parameters() {
        Some(Either::Left(params)) => Some(params.len()),
        Some(Either::Right(num)) => Some(num),

        None => None,
    };

    if let Some(num) = num_parameters {
        if num != input.arg_exprs.len() {
            return Err(
                format!("expected {} parameters, got {}", num, input.arg_exprs.len()).into(),
            );
        }
    }

    let mut warnings = Warnings::default();

    let args_tokens = args::quote_args(&input, config, &mut warnings, &data.describe)?;

    let query_args = format_ident!("query_args");

    let output = if data
        .describe
        .columns()
        .iter()
        .all(|it| it.type_info().is_void())
    {
        let db_path = DB::db_path();
        let sql = &input.sql;

        quote! {
            ::sqlx::__query_with_result::<#db_path, _>(#sql, #query_args)
        }
    } else {
        match input.record_type {
            RecordType::Generated => {
                let columns = output::columns_to_rust::<DB>(&data.describe, config, &mut warnings)?;

                let record_name: Type = syn::parse_str("Record").unwrap();

                for rust_col in &columns {
                    if rust_col.type_.is_wildcard() {
                        return Err(
                            "wildcard overrides are only allowed with an explicit record type, \
                             e.g. `query_as!()` and its variants"
                                .into(),
                        );
                    }
                }

                let record_fields = columns
                    .iter()
                    .map(|output::RustColumn { ident, type_, .. }| quote!(#ident: #type_,));

                let mut record_tokens = quote! {
                    #[derive(Debug)]
                    #[allow(non_snake_case)]
                    struct #record_name {
                        #(#record_fields)*
                    }
                };

                record_tokens.extend(output::quote_query_as::<DB>(
                    &input,
                    &record_name,
                    &query_args,
                    &columns,
                ));

                record_tokens
            }
            RecordType::Given(ref out_ty) => {
                let columns = output::columns_to_rust::<DB>(&data.describe, config, &mut warnings)?;

                output::quote_query_as::<DB>(&input, out_ty, &query_args, &columns)
            }
            RecordType::Scalar => output::quote_query_scalar::<DB>(
                &input,
                config,
                &mut warnings,
                &query_args,
                &data.describe,
            )?,
        }
    };

    let mut warnings_out = TokenStream::new();

    if warnings.ambiguous_datetime {
        // Warns if the date-time crate is inferred but both `chrono` and `time` are enabled
        warnings_out.extend(quote! {
            ::sqlx::warn_on_ambiguous_inferred_date_time_crate();
        });
    }

    if warnings.ambiguous_numeric {
        // Warns if the numeric crate is inferred but both `bigdecimal` and `rust_decimal` are enabled
        warnings_out.extend(quote! {
            ::sqlx::warn_on_ambiguous_inferred_numeric_crate();
        });
    }

    let ret_tokens = quote! {
        {
            #[allow(clippy::all)]
            {
                use ::sqlx::Arguments as _;

                #warnings_out

                #args_tokens

                #output
            }
        }
    };

    if let Some(save_dir) = save_dir {
        data.save_in(save_dir)?;
    }

    Ok(ret_tokens)
}

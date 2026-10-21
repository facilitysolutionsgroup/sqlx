use std::collections::{BTreeSet, HashSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::metadata::{manifest_dir, Metadata};
use crate::opt::ConnectOpts;
use crate::Config;
use anyhow::{bail, Context};
use console::style;
use sqlx::Connection;

pub struct PrepareCtx<'a> {
    pub config: &'a Config,
    pub workspace: bool,
    pub all: bool,
    pub cargo: OsString,
    pub cargo_args: Vec<String>,
    pub metadata: Metadata,
    pub connect_opts: ConnectOpts,
}

impl PrepareCtx<'_> {
    /// Path to the directory where cached queries should be placed.
    fn prepare_dir(&self) -> anyhow::Result<PathBuf> {
        if let Some(dir) = &self.config.common.offline_dir {
            return Ok(if dir.is_absolute() {
                dir.clone()
            } else {
                self.metadata.workspace_root().join(dir)
            });
        }
        if self.workspace {
            Ok(self.metadata.workspace_root().join(".sqlx"))
        } else {
            Ok(manifest_dir(&self.cargo)?.join(".sqlx"))
        }
    }
}

pub async fn run(
    config: &Config,
    check: bool,
    all: bool,
    workspace: bool,
    connect_opts: ConnectOpts,
    cargo_args: Vec<String>,
) -> anyhow::Result<()> {
    let cargo = cargo_command(env::var_os("CARGO"))?;

    anyhow::ensure!(
        Path::new("Cargo.toml").exists(),
        r#"Failed to read `Cargo.toml`.
hint: This command only works in the manifest directory of a Cargo package or workspace."#
    );

    let metadata: Metadata = Metadata::from_current_directory(&cargo)?;
    let workspace = workspace
        || (metadata.current_package().is_none()
            && !Path::new("sqlx.toml").exists()
            && cargo_args.is_empty());
    let mut connect_opts = connect_opts;
    if !workspace {
        connect_opts.populate_db_url(config)?;
    }
    let ctx = PrepareCtx {
        config,
        workspace,
        all,
        cargo,
        cargo_args,
        metadata,
        connect_opts,
    };

    if workspace {
        prepare_workspace(&ctx, check).await
    } else if check {
        prepare_check(&ctx).await
    } else {
        prepare(&ctx).await
    }
}

async fn prepare_workspace(ctx: &PrepareCtx<'_>, check: bool) -> anyhow::Result<()> {
    let groups = workspace_cache_groups(&ctx.metadata, &ctx.cargo_args)?;
    if ctx.connect_opts.database_url.is_some() && groups.len() > 1 {
        bail!("`--database-url` cannot select multiple workspace databases; set each crate's database URL variable instead");
    }
    for group in groups {
        let mut connect_opts = ConnectOpts {
            no_dotenv: crate::opt::NoDotenvOpt {
                no_dotenv: ctx.connect_opts.no_dotenv.no_dotenv,
            },
            database_url: ctx.connect_opts.database_url.clone(),
            connect_timeout: ctx.connect_opts.connect_timeout,
            #[cfg(feature = "_sqlite")]
            sqlite_create_db_wal: ctx.connect_opts.sqlite_create_db_wal,
        };
        connect_opts.populate_db_url(&group.config)?;
        let package_ctx = PrepareCtx {
            config: &group.config,
            workspace: true,
            all: ctx.all,
            cargo: ctx.cargo.clone(),
            cargo_args: group
                .packages
                .iter()
                .flat_map(|name| ["-p".to_owned(), name.clone()])
                .collect(),
            metadata: Metadata::from_current_directory(&ctx.cargo)?,
            connect_opts,
        };
        if check {
            prepare_check(&package_ctx).await?;
        } else {
            prepare(&package_ctx).await?;
        }
    }
    Ok(())
}

struct WorkspaceGroup {
    config: Config,
    cache: PathBuf,
    packages: Vec<String>,
}

fn workspace_cache_groups(
    metadata: &Metadata,
    cargo_args: &[String],
) -> anyhow::Result<Vec<WorkspaceGroup>> {
    if !cargo_args.is_empty() {
        bail!("workspace preparation does not accept Cargo arguments");
    }
    let mut groups: Vec<WorkspaceGroup> = Vec::new();
    for member in metadata.workspace_members() {
        let package = metadata
            .package(member)
            .context("workspace package is missing")?;
        let manifest_dir = package
            .src_paths()
            .first()
            .and_then(|path| path.parent())
            .and_then(Path::parent)
            .context("workspace package has no source directory")?;
        let path = manifest_dir.join("sqlx.toml");
        if path.exists() {
            let config = Config::try_from_path(path)?;
            let cache = config
                .common
                .offline_dir
                .as_ref()
                .map(|dir| {
                    if dir.is_absolute() {
                        dir.clone()
                    } else {
                        metadata.workspace_root().join(dir)
                    }
                })
                .unwrap_or_else(|| manifest_dir.join(".sqlx"));
            if let Some(group) = groups.iter_mut().find(|group| group.cache == cache) {
                if group.config.common.database_url_var() != config.common.database_url_var() {
                    bail!("packages sharing a query cache must use the same database URL variable");
                }
                group.packages.push(package.name().to_owned());
            } else {
                groups.push(WorkspaceGroup {
                    config,
                    cache,
                    packages: vec![package.name().to_owned()],
                });
            }
        }
    }
    if groups.is_empty() {
        bail!("no workspace packages with sqlx.toml found");
    }
    Ok(groups)
}

fn cargo_command(cargo: Option<OsString>) -> anyhow::Result<OsString> {
    cargo.context(
        "the `prepare` subcommand must be invoked as `cargo sqlx prepare`; \
         running `sqlx prepare` directly is not supported",
    )
}

async fn prepare(ctx: &PrepareCtx<'_>) -> anyhow::Result<()> {
    if ctx
        .connect_opts
        .database_url
        .as_deref()
        .is_some_and(needs_preflight)
    {
        check_backend(ctx.config, &ctx.connect_opts).await?;
    }

    let prepare_dir = ctx.prepare_dir()?;
    run_prepare_step(ctx, &prepare_dir)?;

    // Warn if no queries were generated. Glob since the directory may contain unrelated files.
    if glob_query_files(prepare_dir)?.is_empty() {
        println!("{} no queries found", style("warning:").yellow());
        return Ok(());
    }

    if ctx.config.common.offline_dir.is_some() {
        println!(
            "query data written to {}; please check this into version control",
            ctx.prepare_dir()?.display()
        );
    } else if ctx.workspace {
        println!(
            "query data written to .sqlx in the workspace root; \
             please check this into version control"
        );
    } else {
        println!(
            "query data written to .sqlx in the current directory; \
             please check this into version control"
        );
    }
    Ok(())
}

async fn prepare_check(ctx: &PrepareCtx<'_>) -> anyhow::Result<()> {
    if ctx
        .connect_opts
        .database_url
        .as_deref()
        .is_some_and(needs_preflight)
    {
        check_backend(ctx.config, &ctx.connect_opts).await?;
    }

    // Re-generate and store the queries in a separate directory from both the prepared
    // queries and the ones generated by `cargo check`, to avoid conflicts.
    let prepare_dir = ctx.prepare_dir()?;
    let cache_dir = ctx.metadata.target_directory().join("sqlx-prepare-check");
    run_prepare_step(ctx, &cache_dir)?;

    // Compare .sqlx to cache.
    let prepare_filenames: HashSet<String> = glob_query_files(&prepare_dir)?
        .into_iter()
        .filter_map(|path| path.file_name().map(|f| f.to_string_lossy().into_owned()))
        .collect();
    let cache_filenames: HashSet<String> = glob_query_files(&cache_dir)?
        .into_iter()
        .filter_map(|path| path.file_name().map(|f| f.to_string_lossy().into_owned()))
        .collect();

    // Error: files in cache but not .sqlx.
    if cache_filenames
        .difference(&prepare_filenames)
        .next()
        .is_some()
    {
        bail!("prepare check failed: .sqlx is missing one or more queries; you should re-run sqlx prepare");
    }
    // Warn: files in .sqlx but not cache.
    if prepare_filenames
        .difference(&cache_filenames)
        .next()
        .is_some()
    {
        println!(
            "{} potentially unused queries found in .sqlx; you may want to re-run sqlx prepare",
            style("warning:").yellow()
        );
    }

    // Compare file contents as JSON to ignore superficial differences.
    // Everything in cache checked to be in .sqlx already.
    for filename in cache_filenames {
        let prepare_json = load_json_file(prepare_dir.join(&filename))?;
        let cache_json = load_json_file(cache_dir.join(&filename))?;
        if prepare_json != cache_json {
            bail!("prepare check failed: one or more query files differ ({}); you should re-run sqlx prepare", filename);
        }
    }

    Ok(())
}

fn run_prepare_step(ctx: &PrepareCtx, cache_dir: &Path) -> anyhow::Result<()> {
    // Create and/or clean the directory.
    fs::create_dir_all(cache_dir).context(format!(
        "Failed to create query cache directory: {:?}",
        cache_dir
    ))?;

    // Create directory to hold temporary query files before they get persisted to SQLX_OFFLINE_DIR
    let tmp_dir = ctx.metadata.target_directory().join("sqlx-tmp");
    fs::create_dir_all(&tmp_dir).context(format!(
        "Failed to create temporary query cache directory: {:?}",
        tmp_dir
    ))?;

    // Only delete sqlx-*.json files to avoid accidentally deleting any user data.
    for query_file in glob_query_files(cache_dir).context("Failed to read query cache files")? {
        fs::remove_file(&query_file)
            .with_context(|| format!("Failed to delete query file: {}", query_file.display()))?;
    }

    // Try only triggering a recompile on crates that use `sqlx-macros` falling back to a full
    // clean on error
    setup_minimal_project_recompile(&ctx.cargo, &ctx.metadata, ctx.all, ctx.workspace)?;

    // Compile the queries.
    let check_status = {
        let mut check_command = Command::new(&ctx.cargo);
        check_command
            .arg("check")
            .args(&ctx.cargo_args)
            .env("SQLX_TMP", tmp_dir)
            .env("SQLX_OFFLINE", "false")
            .env("SQLX_OFFLINE_DIR", cache_dir);

        if let Some(database_url) = &ctx.connect_opts.database_url {
            check_command.env(ctx.config.common.database_url_var(), database_url);
        }
        if ctx.config.common.offline_dir.is_some()
            && ctx.config.common.database_url_var() != "DATABASE_URL"
        {
            check_command.env_remove("DATABASE_URL");
        }

        // `cargo check` recompiles on changed rust flags which can be set either via the env var
        // or through the `rustflags` field in `$CARGO_HOME/config` when the env var isn't set.
        // Because of this we only pass in `$RUSTFLAGS` when present.
        if let Ok(rustflags) = env::var("RUSTFLAGS") {
            check_command.env("RUSTFLAGS", rustflags);
        }

        check_command.status()?
    };
    if !check_status.success() {
        bail!("`cargo check` failed with status: {}", check_status);
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
struct ProjectRecompileAction {
    // The names of the packages
    clean_packages: Vec<String>,
    touch_paths: Vec<PathBuf>,
}

/// Sets up recompiling only crates that depend on `sqlx-macros`
///
/// This gets a listing of all crates that depend on `sqlx-macros` (direct and transitive). The
/// crates within the current workspace have their source file's mtimes updated while crates
/// outside the workspace are selectively `cargo clean -p`ed. In this way we can trigger a
/// recompile of crates that may be using compile-time macros without forcing a full recompile.
///
/// If `workspace` is false, only the current package will have its files' mtimes updated.
fn setup_minimal_project_recompile(
    cargo: impl AsRef<OsStr>,
    metadata: &Metadata,
    all: bool,
    workspace: bool,
) -> anyhow::Result<()> {
    let recompile_action: ProjectRecompileAction = if workspace {
        minimal_project_recompile_action(metadata, all)
    } else {
        // Only touch the current crate.
        ProjectRecompileAction {
            clean_packages: Vec::new(),
            touch_paths: metadata.current_package()
                .context("failed to get package in current working directory, pass `--workspace` if running from a workspace root")?
                .src_paths()
                .to_vec(),
        }
    };

    if let Err(err) = minimal_project_clean(&cargo, recompile_action) {
        println!(
            "Failed minimal recompile setup. Cleaning entire project. Err: {}",
            err
        );
        let clean_status = Command::new(&cargo).arg("clean").status()?;
        if !clean_status.success() {
            bail!("`cargo clean` failed with status: {}", clean_status);
        }
    }

    Ok(())
}

fn minimal_project_clean(
    cargo: impl AsRef<OsStr>,
    action: ProjectRecompileAction,
) -> anyhow::Result<()> {
    let ProjectRecompileAction {
        clean_packages,
        touch_paths,
    } = action;

    // Update the modified timestamp of package files to force a selective recompilation.
    for file in touch_paths {
        let now = filetime::FileTime::now();
        filetime::set_file_times(&file, now, now)
            .with_context(|| format!("Failed to update mtime for {file:?}"))?;
    }

    // Clean entire packages.
    for pkg_id in &clean_packages {
        let clean_status = Command::new(&cargo)
            .args(["clean", "-p", pkg_id])
            .status()?;

        if !clean_status.success() {
            bail!("`cargo clean -p {}` failed", pkg_id);
        }
    }

    Ok(())
}

fn minimal_project_recompile_action(metadata: &Metadata, all: bool) -> ProjectRecompileAction {
    // Get all the packages that depend on `sqlx-macros`
    let mut sqlx_macros_dependents = BTreeSet::new();
    let sqlx_macros_ids: BTreeSet<_> = metadata
        .entries()
        // We match just by name instead of name and url because some people may have it installed
        // through different means like vendoring
        .filter(|(_, package)| package.name() == "sqlx-macros")
        .map(|(id, _)| id)
        .collect();
    for sqlx_macros_id in sqlx_macros_ids {
        sqlx_macros_dependents.extend(metadata.all_dependents_of(sqlx_macros_id));
    }

    // Figure out which `sqlx-macros` dependents are in the workspace vs out
    let mut in_workspace_dependents = Vec::new();
    let mut out_of_workspace_dependents = Vec::new();
    for dependent in sqlx_macros_dependents {
        if metadata.workspace_members().contains(dependent) {
            in_workspace_dependents.push(dependent);
        } else {
            out_of_workspace_dependents.push(dependent);
        }
    }

    // In-workspace dependents have their source file's mtime updated.
    let files_to_touch: Vec<_> = in_workspace_dependents
        .iter()
        .filter_map(|id| {
            metadata
                .package(id)
                .map(|package| package.src_paths().to_owned())
        })
        .flatten()
        .collect();

    // Out-of-workspace get `cargo clean -p <PKGID>`ed, only if --all is set.
    let packages_to_clean: Vec<_> = if all {
        out_of_workspace_dependents
            .iter()
            .filter_map(|id| {
                metadata
                    .package(id)
                    .map(|package| package.name().to_owned())
            })
            // Do not clean sqlx, it depends on sqlx-macros but has no queries to prepare itself.
            .filter(|name| name != "sqlx")
            .collect()
    } else {
        Vec::new()
    };

    ProjectRecompileAction {
        clean_packages: packages_to_clean,
        touch_paths: files_to_touch,
    }
}

/// Find all `query-*.json` files in a directory.
fn glob_query_files(path: impl AsRef<Path>) -> anyhow::Result<Vec<PathBuf>> {
    let path = path.as_ref();
    let pattern = path.join("query-*.json");
    glob::glob(
        pattern
            .to_str()
            .context("query cache path is invalid UTF-8")?,
    )
    .with_context(|| format!("failed to read query cache path: {}", path.display()))?
    .collect::<Result<Vec<_>, _>>()
    .context("glob failed")
}

/// Load the JSON contents of a query data file.
fn load_json_file(path: impl AsRef<Path>) -> anyhow::Result<serde_json::Value> {
    let path = path.as_ref();
    let file_bytes =
        fs::read(path).with_context(|| format!("failed to load file: {}", path.display()))?;
    Ok(serde_json::from_slice(&file_bytes)?)
}

fn needs_preflight(url: &str) -> bool {
    !url.starts_with("odbc:")
}

async fn check_backend(config: &Config, opts: &ConnectOpts) -> anyhow::Result<()> {
    crate::connect(config, opts).await?.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_eq;

    #[test]
    fn workspace_prepare_groups_crates_by_cache() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let root = root.path();
        std::fs::create_dir_all(root.join("b_in_workspace_lib"))?;
        std::fs::create_dir_all(root.join("c_in_workspace_bin"))?;
        let config =
            "[common]\ndatabase-url-var = 'FIRST_DATABASE_URL'\noffline-dir = 'cache/first'\n";
        std::fs::write(root.join("b_in_workspace_lib/sqlx.toml"), config)?;
        std::fs::write(root.join("c_in_workspace_bin/sqlx.toml"), config)?;
        let sample_metadata = std::fs::read_to_string("tests/assets/sample_metadata.json")?;
        let sample_metadata =
            sample_metadata.replace("/home/user/problematic/workspace", &root.to_string_lossy());
        let metadata: Metadata = sample_metadata.parse()?;
        let groups = workspace_cache_groups(&metadata, &[])?;
        assert!(groups.iter().any(|group| group
            .packages
            .contains(&"b_in_workspace_lib".to_owned())
            && group.packages.contains(&"c_in_workspace_bin".to_owned())));
        Ok(())
    }

    #[test]
    fn workspace_prepare_rejects_extra_cargo_arguments() -> anyhow::Result<()> {
        let sample_metadata = std::fs::read_to_string("tests/assets/sample_metadata.json")?;
        let metadata: Metadata = sample_metadata.parse()?;
        let error = workspace_cache_groups(&metadata, &["--all-features".to_owned()])
            .err()
            .context("workspace arguments must be rejected")?;
        assert_eq!(
            error.to_string(),
            "workspace preparation does not accept Cargo arguments"
        );
        Ok(())
    }

    #[test]
    fn odbc_prepare_skips_any_connection_preflight() {
        assert!(!needs_preflight("odbc://?connection_string=DSN%3DExample"));
        assert!(!needs_preflight("odbc:DSN=Example"));
        assert!(needs_preflight("postgres://localhost/db"));
    }

    #[test]
    fn missing_cargo_environment_explains_prepare_invocation() {
        let error = cargo_command(None).expect_err("missing CARGO should fail");

        assert_eq!(
            error.to_string(),
            "the `prepare` subcommand must be invoked as `cargo sqlx prepare`; running `sqlx prepare` directly is not supported"
        );
    }

    #[test]
    fn minimal_project_recompile_action_works() -> anyhow::Result<()> {
        let sample_metadata_path = Path::new("tests")
            .join("assets")
            .join("sample_metadata.json");
        let sample_metadata = std::fs::read_to_string(sample_metadata_path)?;
        let metadata: Metadata = sample_metadata.parse()?;

        let action = minimal_project_recompile_action(&metadata, false);
        assert_eq!(
            action,
            ProjectRecompileAction {
                clean_packages: vec![],
                touch_paths: vec![
                    "/home/user/problematic/workspace/b_in_workspace_lib/src/lib.rs".into(),
                    "/home/user/problematic/workspace/c_in_workspace_bin/src/main.rs".into(),
                ],
            }
        );

        Ok(())
    }
}

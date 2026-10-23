//! `moirai-router`: one Zenoh session in router mode, from
//! [`moirai_router::config`] (or [`moirai_router::config_with_regions`] given
//! `ROUTER_REGIONS`), running until it is killed.
//!
//! - `LISTEN_PORT` — the TCP port it listens on, on every IPv4 interface;
//!   unset or empty, [`moirai_router::DEFAULT_PORT`]; 0, a port the kernel
//!   chooses
//! - `ROUTER_REGIONS` — a comma-separated list of region names, each
//!   non-empty, at most 32 bytes, none twice: one Zenoh subregion per name,
//!   in order, then a catch-all for a node of any other region or of none.
//!   Unset, Zenoh's `auto` preset. An invalid list ends the process non-zero,
//!   naming the variable; a value set but empty is a list of one empty name,
//!   so it is refused too
//! - `PORTS_FILE` — once the session is open, the router writes
//!   `{"pid", "listen_port"}` there (its pid and the port its session listens
//!   on), plus `"regions"`, the names it applied in order, when
//!   `ROUTER_REGIONS` is set; beside the path and renamed onto it so it
//!   appears whole
//!
//! It reads no other variable: no `ZENOH_*`, no configuration file, so
//! nothing in its environment can turn multicast scouting back on.

use std::env;
use std::ffi::OsString;
use std::path::Path;
use std::process::exit;
use std::thread;

use zenoh::Wait;

/// `LISTEN_PORT`, or the end of the process naming it.
fn listen_port() -> u16 {
    match env::var("LISTEN_PORT") {
        Ok(raw) if raw.trim().is_empty() => moirai_router::DEFAULT_PORT,
        Ok(raw) => raw.trim().parse().unwrap_or_else(|err| {
            eprintln!("[moirai-router] invalid LISTEN_PORT `{raw}`: {err}");
            exit(1);
        }),
        Err(env::VarError::NotPresent) => moirai_router::DEFAULT_PORT,
        Err(err) => {
            eprintln!("[moirai-router] invalid LISTEN_PORT: {err}");
            exit(1);
        }
    }
}

/// The names of `ROUTER_REGIONS`, `None` when it is unset, or the end of
/// the process naming it.
fn router_regions() -> Option<Vec<String>> {
    let raw = match env::var(moirai_router::ROUTER_REGIONS) {
        Ok(raw) => raw,
        Err(env::VarError::NotPresent) => return None,
        Err(err) => {
            eprintln!(
                "[moirai-router] invalid {}: {err}",
                moirai_router::ROUTER_REGIONS
            );
            exit(1);
        }
    };
    Some(moirai_router::regions(&raw).unwrap_or_else(|err| {
        eprintln!("[moirai-router] {err}");
        exit(1);
    }))
}

/// The port `session` listens on, read from its locators: the kernel's
/// choice after a bind to port 0. `None` when it lists no locator, as a
/// session on 0.0.0.0 does in a namespace whose only address is loopback.
fn session_port(session: &zenoh::Session) -> Option<u16> {
    session.info().locators().wait().iter().find_map(|locator| {
        locator
            .address()
            .as_str()
            .parse::<std::net::SocketAddr>()
            .ok()
            .map(|addr| addr.port())
    })
}

/// Write the report to `path`, beside it first and renamed onto it.
fn write_ports_file(
    path: &Path,
    listen_port: u16,
    regions: Option<&[String]>,
) -> std::io::Result<()> {
    let regions = regions
        .map(|names| format!(",\"regions\":{}", moirai_router::json_strings(names)))
        .unwrap_or_default();
    let report = format!(
        "{{\"pid\":{},\"listen_port\":{listen_port}{regions}}}",
        std::process::id()
    );
    let mut staging = OsString::from(path.as_os_str());
    staging.push(".staging");
    std::fs::write(&staging, report)?;
    std::fs::rename(&staging, path)
}

fn main() {
    let given = listen_port();
    let regions = router_regions();
    let config = match &regions {
        Some(names) => moirai_router::config_with_regions(given, names),
        None => moirai_router::config(given),
    }
    .unwrap_or_else(|err| {
        eprintln!("[moirai-router] {err}");
        exit(1);
    });
    let session = zenoh::open(config).wait().unwrap_or_else(|err| {
        eprintln!("[moirai-router] cannot open the router session on port {given}: {err}");
        exit(1);
    });
    let port = match session_port(&session) {
        Some(port) => port,
        None if given != 0 => given,
        None => {
            eprintln!(
                "[moirai-router] the session lists no locator, so the port the kernel chose \
                 for LISTEN_PORT=0 is unknown; give a fixed LISTEN_PORT"
            );
            exit(1);
        }
    };
    if let Some(path) = env::var_os("PORTS_FILE").filter(|p| !p.is_empty())
        && let Err(err) = write_ports_file(Path::new(&path), port, regions.as_deref())
    {
        eprintln!(
            "[moirai-router] cannot write PORTS_FILE `{}`: {err}",
            Path::new(&path).display()
        );
        exit(1);
    }
    let layout = match &regions {
        Some(names) => format!("regions {names:?} then a catch-all"),
        None => "Zenoh's auto preset".to_string(),
    };
    eprintln!(
        "[moirai-router] {} listening on port {port}, multicast scouting off, {layout}",
        session.zid()
    );
    // The session lives as long as the process; it ends when it is killed.
    loop {
        thread::park();
    }
}

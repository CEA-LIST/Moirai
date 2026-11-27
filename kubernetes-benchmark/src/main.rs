use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

fn collect_configs(directory: &Path, configs: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        if kind.is_dir() {
            collect_configs(&path, configs)?;
        } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "json") {
            configs.push(path);
        }
    }
    Ok(())
}

fn run() -> io::Result<bool> {
    let mut args = env::args_os().skip(1);
    let (Some(configs), Some(results), None) = (args.next(), args.next(), args.next()) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: kubernetes-benchmark <configuration-folder> <new-results-folder>",
        ));
    };
    let configs = PathBuf::from(configs).canonicalize()?;
    let results = PathBuf::from(results);
    let mut trials = Vec::new();
    collect_configs(&configs, &mut trials)?;
    trials.sort();
    if trials.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no JSON configurations found",
        ));
    }
    if let Some(parent) = results.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(&results)?;
    let launcher = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../kubernetes-launcher/benchmark-replica/benchmark_launch.sh");
    let mut report = fs::File::create(results.join("suite.tsv"))?;
    writeln!(report, "config\tresult_folder\tstatus")?;
    let mut successful = true;
    for (index, config) in trials.iter().enumerate() {
        let name = config.file_stem().unwrap().to_string_lossy();
        let output = format!("{:04}-{name}", index + 1);
        println!(
            "Running {} of {} ({})",
            index + 1,
            trials.len(),
            config.display()
        );
        let status = Command::new("bash")
            .arg(&launcher)
            .arg(config)
            .arg("--output")
            .arg(results.join(&output))
            .status()?;
        writeln!(
            report,
            "{}\t{output}\t{status}",
            config.strip_prefix(&configs).unwrap().display()
        )?;
        successful &= status.success();
        if status.code() == Some(130) || status.code().is_none() {
            break;
        }
    }
    Ok(successful)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

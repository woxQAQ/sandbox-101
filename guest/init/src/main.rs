fn main() {
    if let Err(error) = sandbox_init::boot() {
        eprintln!("sandbox-init: fatal: {error:#}");
        // Returning from PID 1 deliberately produces a kernel panic and leaves
        // the serial diagnostic visible to the host. There is no degraded boot.
        std::process::exit(1);
    }
}

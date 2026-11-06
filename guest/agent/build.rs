fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=../../protocol/sandbox.proto");
    prost_build::compile_protos(&["../../protocol/sandbox.proto"], &["../../protocol"])
}

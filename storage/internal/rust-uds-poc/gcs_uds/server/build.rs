fn main() {
    println!("cargo:rerun-if-changed=../proto/gcscore.proto");
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(true)
        .bytes(".")
        .compile_protos(&["../proto/gcscore.proto"], &["../proto"])
        .expect("compile gcscore.proto");
}

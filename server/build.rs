fn main() {
    // Gera os tipos Rust a partir do mesmo messages.proto usado pelo bot Python.
    prost_build::compile_protos(&["proto/messages.proto"], &["proto/"]).unwrap();
}
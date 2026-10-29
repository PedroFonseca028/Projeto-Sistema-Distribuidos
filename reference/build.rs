fn main() {
    // Gera os tipos Rust a partir do mesmo messages.proto usado pelos outros componentes.
    prost_build::compile_protos(&["proto/messages.proto"], &["proto/"]).unwrap();
}
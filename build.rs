fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    if std::env::var("TARGET").as_deref() == Ok("aarch64-pc-windows-msvc") {
        // The Windows loader sizes the initial thread from the PE header.
        // Give it the same 8 MiB stack reserve as Bamboo's spawned workers.
        println!("cargo:rustc-link-arg-bin=bamboo=/STACK:8388608");
    }
}

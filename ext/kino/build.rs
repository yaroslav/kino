pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = rb_sys_env::activate()?;

    // cfg(kino_thread_safe_free) where this Ruby's headers declare the flag (src/typed_data.rs).
    println!("cargo:rustc-check-cfg=cfg(kino_thread_safe_free)");
    // Rerun when this script, the header or where it is changes, or with rb-sys's build (a new
    // Ruby); not on every edit of the crate's sources.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RBCONFIG_rubyhdrdir");
    // The header directory as rb-sys itself looks it up: the rb_sys gem's environment first,
    // which is all a cross build has, then what rb-sys read from the Ruby it ran.
    let header = std::env::var("RBCONFIG_rubyhdrdir")
        .ok()
        .or_else(|| env.get_rbconfig_value("RUBYHDRDIR").map(String::from))
        .map(|dir| format!("{}/ruby/internal/core/rtypeddata.h", dir.trim()));
    let declared = header.as_ref().is_some_and(|header| {
        println!("cargo:rerun-if-changed={header}");
        std::fs::read_to_string(header)
            .is_ok_and(|text| text.contains("RUBY_TYPED_THREAD_SAFE_FREE"))
    });
    if declared {
        println!("cargo:rustc-cfg=kino_thread_safe_free");
    } else if env.ruby_major_minor() >= (4, 1) {
        // A warning, not an error: without the flag Kino is slower on this Ruby, not broken.
        let looked = header.map_or_else(
            || "no header directory from rb-sys".to_string(),
            |header| format!("not in '{header}'"),
        );
        println!(
            "cargo:warning=no RUBY_TYPED_THREAD_SAFE_FREE ({looked}): this Ruby frees Kino's \
             native objects in batches under a VM barrier"
        );
    }

    Ok(())
}

#[path = "../manual/publication.rs"]
mod publication;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 || args[0] != "--output" {
        return Err("usage: export_manual_site --output NEW_DIRECTORY".into());
    }
    let root = publication::write_new(
        std::path::Path::new(&args[1]),
        bamboo_skills::store::builtin::BUILTIN_SKILL_FILES,
    )?;
    println!("{}", root.display());
    Ok(())
}

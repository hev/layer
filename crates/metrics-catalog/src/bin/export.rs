fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut docs: Vec<_> = metrics_catalog::catalog().iter().collect();
    docs.sort_by_key(|doc| doc.name);

    serde_json::to_writer_pretty(std::io::stdout(), &docs)?;
    println!();
    Ok(())
}

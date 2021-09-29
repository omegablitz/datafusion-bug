use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use datafusion::datasource::MemTable;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let mut ctx = ExecutionContext::new();
    {
        let json_file = datafusion::datasource::json::NdJsonFile::try_new(
            "table_1.json",
            datafusion::physical_plan::json::NdJsonReadOptions::default(),
        )?;
        let provider = MemTable::load(Arc::new(json_file), 99999, None).await?;
        ctx.register_table("table_1", Arc::new(provider))?;
    }
    {
        let json_file = datafusion::datasource::json::NdJsonFile::try_new(
            "table_2.json",
            datafusion::physical_plan::json::NdJsonReadOptions::default(),
        )?;
        let provider = MemTable::load(Arc::new(json_file), 99999, None).await?;
        ctx.register_table("table_2", Arc::new(provider))?;
    }

    let query = r#"
      SELECT SUM(c), SUM(d)
      FROM (
        SELECT a, b, c, 0.0 AS d FROM table_1
        UNION ALL
        SELECT a, b, 0.0 AS c, d FROM table_2
      )
    "#;

    let df = ctx.sql(query)?;

    let results: Vec<RecordBatch> = df.collect().await?;

    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?;

    println!("{}", pretty_results);

    Ok(())
}

// Execute a SQL template file with optional parameters for substitution
// Replace instances of :param with the corresponding value from params
#[tracing::instrument(
    skip(client, sql_file, params)
)]
pub async fn execute_sql_template(
    client: &tokio_postgres::Client,
    sql_file: std::path::PathBuf,
    // Additional parameters for the SQL template for substitution
    // e.g., Dictionary of parameters like `{"param1": value1, "param2": value2}`
    params: Option<std::collections::HashMap<String, String>>,
) -> anyhow::Result<()> {
    let mut sql = tokio::fs::read_to_string(&sql_file).await?;

    if let Some(params) = params {
        for (key, value) in params {
            // Replace :key with the value in the SQL string
            let placeholder = format!(":{}", key);
            sql = sql.replace(&placeholder, &value);
            tracing::debug!("Replaced {} with {}", placeholder, value);
        }
    }

    tracing::debug!("Executing SQL:\n{}", sql);

    client.batch_execute(&sql).await?;
    tracing::info!("SQL executed successfully");

    Ok(())
}

// Execute a SQL template file with optional parameters for substitution
// Replace instances of :param with the corresponding value from params
#[tracing::instrument(
    skip(client, sql_query, params)
)]
pub async fn execute_sql_template_str(
    client: &tokio_postgres::Client,
    sql_query: &str,
    // Additional parameters for the SQL template for substitution
    // e.g., Dictionary of parameters like `{"param1": value1, "param2": value2}`
    params: Option<std::collections::HashMap<String, String>>,
) -> anyhow::Result<()> {
    let mut sql = sql_query.to_string();

    if let Some(params) = params {
        for (key, value) in params {
            // Replace :key with the value in the SQL string
            let placeholder = format!(":{}", key);
            sql = sql.replace(&placeholder, &value);
            tracing::debug!("Replaced {} with {}", placeholder, value);
        }
    }

    tracing::debug!("Executing SQL:\n{}", sql);

    client.batch_execute(&sql).await?;
    tracing::info!("SQL executed successfully");

    Ok(())
}
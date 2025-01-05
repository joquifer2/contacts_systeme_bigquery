from google.cloud import bigquery

# Configuración
PROJECT_ID = "jordi-quiroga"
DATASET_ID = "systeme"
TABLE_ID = "contacts"  # Nombre de la tabla
SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("registeredAt", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("locale", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sourceURL", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unsubscribed", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("bounced", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("needsConfirmation", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField(
        "fields", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("surname", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("postcode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("street_address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("phone_number", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("company_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tax_number", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("campo_de_texto", "STRING", mode="NULLABLE"),
        ]
    ),
    bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
]

def create_table():
    """
    Crea una tabla en BigQuery con el esquema especificado.
    """
    client = bigquery.Client(project=PROJECT_ID)

    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    table = bigquery.Table(table_ref, schema=SCHEMA)

    try:
        table = client.create_table(table)  # Crea la tabla
        print(f"Tabla {TABLE_ID} creada en el dataset {DATASET_ID}.")
    except Exception as e:
        print(f"Error al crear la tabla: {e}")

if __name__ == "__main__":
    create_table()

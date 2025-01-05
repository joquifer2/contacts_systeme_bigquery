import requests
from google.cloud import bigquery
from google.cloud.exceptions import Conflict
from config import SYSTEME_API_KEY, SYSTEME_CONTACTS_URL, SYSTEME_TAGS_URL

def get_contacts(api_key, endpoint):
    """
    Obtiene contactos desde la API de systeme.io.
    """
    headers = {
        "accept": "application/json",
        "X-API-Key": api_key
    }
    contacts = []

    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()

        if "items" in data:
            contacts = data["items"]
            print(f"Se obtuvieron {len(contacts)} contactos.")
        else:
            print("No se encontraron contactos en la respuesta.")
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener contactos: {e}")

    return contacts

def get_tags(api_key, endpoint):
    """
    Obtiene etiquetas desde la API de systeme.io.
    """
    headers = {
        "accept": "application/json",
        "X-API-Key": api_key
    }
    tags = {}

    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()

        if "items" in data:
            for tag in data["items"]:
                tags[tag["id"]] = tag["name"]
            print(f"Se obtuvieron {len(tags)} etiquetas.")
        else:
            print("No se encontraron etiquetas en la respuesta.")
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener etiquetas: {e}")

    return tags

def process_contacts(contacts, tags):
    """
    Procesa la lista de contactos para adaptarla al formato de BigQuery.
    """
    processed = []
    for contact in contacts:
        # Convierte fields a un diccionario basado en el slug
        fields_dict = {field["slug"]: field["value"] for field in contact.get("fields", [])}
        
        processed.append({
            "id": contact.get("id"),
            "email": contact.get("email"),
            "registeredAt": contact.get("registeredAt"),
            "locale": contact.get("locale"),
            "sourceURL": contact.get("sourceURL"),
            "unsubscribed": contact.get("unsubscribed"),
            "bounced": contact.get("bounced"),
            "needsConfirmation": contact.get("needsConfirmation"),
            "fields": {
                "first_name": fields_dict.get("first_name"),
                "surname": fields_dict.get("surname"),
                "country": fields_dict.get("country"),
                "city": fields_dict.get("city"),
                "postcode": fields_dict.get("postcode"),
                "street_address": fields_dict.get("street_address"),
                "phone_number": fields_dict.get("phone_number"),
                "company_name": fields_dict.get("company_name"),
                "tax_number": fields_dict.get("tax_number"),
                "state": fields_dict.get("state"),
                "campo_de_texto": fields_dict.get("campo_de_texto"),
            },
            "tags": [tags.get(tag["id"], "Etiqueta desconocida") for tag in contact.get("tags", [])]
        })
    return processed



def insert_temp_table(project_id, dataset_id, temp_table_id, rows_to_insert):
    """
    Inserta datos en una tabla temporal en BigQuery.
    """
    client = bigquery.Client(project=project_id)
    temp_table_ref = client.dataset(dataset_id).table(temp_table_id)

    # Define el esquema de la tabla temporal
    schema = [
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

    temp_table = bigquery.Table(temp_table_ref, schema=schema)

    try:
        # Intenta crear la tabla
        client.create_table(temp_table)
        print(f"Tabla temporal '{temp_table_id}' creada.")
    except Conflict:  # Si la tabla ya existe
        print(f"La tabla temporal '{temp_table_id}' ya existe. Continuando...")

    # Inserta los datos en la tabla temporal
    errors = client.insert_rows_json(temp_table_ref, rows_to_insert)
    if errors:
        raise RuntimeError(f"Errores al insertar en la tabla temporal: {errors}")
    


def merge_tables(project_id, dataset_id, temp_table_id, target_table_id):
    """
    Realiza un MERGE entre la tabla temporal y la tabla principal en BigQuery.
    """
    client = bigquery.Client(project=project_id)

    # MERGE con eliminación de duplicados en la tabla temporal
    query = f"""
    MERGE `{project_id}.{dataset_id}.{target_table_id}` AS target
    USING (
        SELECT 
            * EXCEPT(row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY registeredAt DESC) AS row_num
            FROM `{project_id}.{dataset_id}.{temp_table_id}`
        )
        WHERE row_num = 1
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN
      UPDATE SET
        email = source.email,
        registeredAt = source.registeredAt,
        locale = source.locale,
        sourceURL = source.sourceURL,
        unsubscribed = source.unsubscribed,
        bounced = source.bounced,
        needsConfirmation = source.needsConfirmation,
        fields = source.fields,
        tags = source.tags
    WHEN NOT MATCHED THEN
      INSERT (id, email, registeredAt, locale, sourceURL, unsubscribed, bounced, needsConfirmation, fields, tags)
      VALUES (source.id, source.email, source.registeredAt, source.locale, source.sourceURL, source.unsubscribed, source.bounced, source.needsConfirmation, source.fields, source.tags);
    """

    # Ejecutar el MERGE
    client.query(query).result()

# Una vez terminado el merge, limpiamos la tabla temporal
    # Eliminamos la tabla para que no queden registros residuales.
    try:
        client.delete_table(
            f"{project_id}.{dataset_id}.{temp_table_id}",
            not_found_ok=True  # Para evitar error si la tabla no existe
        )
        print(f"La tabla temporal '{temp_table_id}' ha sido eliminada.")
    except Exception as e:
        print(f"Error al eliminar la tabla temporal: {e}")

def main(request):
    """
    Punto de entrada para la Cloud Function.
    """
    print("Obteniendo etiquetas desde systeme.io...")
    tags = get_tags(SYSTEME_API_KEY, SYSTEME_TAGS_URL)

    print("Obteniendo contactos desde systeme.io...")
    contacts = get_contacts(SYSTEME_API_KEY, SYSTEME_CONTACTS_URL)

    if contacts:
        print("Procesando contactos...")
        processed_contacts = process_contacts(contacts, tags)
        
        print("Insertando contactos en tabla temporal...")
        insert_temp_table("jordi-quiroga", "systeme", "temp_contacts", processed_contacts)
        
        print("Realizando MERGE con la tabla principal...")
        merge_tables("jordi-quiroga", "systeme", "temp_contacts", "contacts")
        
        return "Sincronización completada", 200
    else:
        print("No se encontraron contactos para procesar.")
        return "Sincronización completada", 200

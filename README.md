# Integración Automatizada de Contactos de Systeme.io con Google BigQuery

Este proyecto ofrece una solución automatizada para sincronizar los contactos y etiquetas de **Systeme.io** con **Google BigQuery**. Su objetivo es centralizar los datos, facilitar el análisis avanzado y permitir la integración con otras fuentes de información para obtener una **visión global** del negocio.

---

## ¿Qué hace este código?

### 1. **Conecta con la API de Systeme.io**

- **Descarga contactos** y sus detalles, como nombre, email, etiquetas y campos personalizados.
- **Obtiene las etiquetas** creadas en Systeme.io para asociarlas correctamente con cada contacto.

### 2. **Procesa los datos**

- Estructura y transforma los datos descargados para adaptarlos al esquema de **Google BigQuery**.
- Clasifica la información por campos personalizados y organiza etiquetas asociadas.

### 3. **Carga los datos en BigQuery**

- **Crea una tabla temporal** en BigQuery para almacenar los nuevos datos antes de procesarlos.
- **Realiza un MERGE** con la tabla principal, actualizando registros existentes e insertando nuevos contactos.
- **Elimina la tabla temporal** después de completar el proceso para mantener el entorno limpio.

### 4. **Actualiza y sincroniza automáticamente**

- Cada vez que se ejecuta el código, descarga nuevos datos, los procesa y los integra en BigQuery.
- Asegura que los datos estén siempre actualizados para su análisis en tiempo real.

---

## Ventajas de esta integración

- **Automatización completa:** Sincroniza nuevos contactos y etiquetas sin intervención manual.
- **Organización eficiente:** Centraliza datos dispersos para un análisis más rápido y preciso.
- **Visión global del negocio:** Permite combinar los datos de **Systeme.io** con otras fuentes, como **Meta Ads**, **Google Ads** y **Google Analytics**.
- **Preparación para análisis avanzados:** Compatible con herramientas de visualización como **Looker Studio**.
- **Escalabilidad garantizada:** Diseñado para gestionar grandes volúmenes de información.

---

## Requisitos previos

1. **Google Cloud Platform (GCP):**
   - Proyecto configurado en GCP.
   - API de BigQuery habilitada.
2. **Credenciales de GCP:**
   - Archivo JSON con claves de servicio configurado para la autenticación.
3. **API de Systeme.io:**
   - Clave API válida para acceder a los datos de contactos y etiquetas.
4. **Dependencias de Python:**
   - `google-cloud-bigquery`
   - `requests`

---

## Licencia
Este proyecto está disponible bajo la licencia [MIT](LICENSE).

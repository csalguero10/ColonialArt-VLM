# ARCA → Google Sheets scraper

Script para extraer metadatos de obras del archivo ARCA
(`arca.uniandes.edu.co`) y escribirlos en google sheet `inventory_metadata`.

## 1. Instalar dependencias

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium        # navegador que usa Playwright
```

## 2. Credenciales de Google Sheets (service account)

Gspread necesita una cuenta de servicio. Una vez y queda para siempre:

1. Entra a <https://console.cloud.google.com/>, crea (o reutiliza) un
   proyecto, y habilita **Google Sheets API** y **Google Drive API**.
2. `IAM & Admin → Service accounts → Create service account`. Nombre
   cualquiera (ej. `arca-scraper`).
3. En la pestaña **Keys → Add key → Create new key → JSON**. Se descarga
   un JSON; renómbralo a `credentials.json` y ponlo al lado del script.
4. Abre el JSON, copia el campo `client_email` (algo como
   `arca-scraper@tu-proyecto.iam.gserviceaccount.com`) y **compártele los
   dos sheets con permiso de editor**:
   - `links`  → solo lectura es suficiente
   - `inventory_metadata` → editor

## 3. Correr

```bash
python arca_scraper.py
```

El script:

1. Lee los URLs de la columna A del sheet `links`.
2. Renderiza cada página con Chromium headless (necesario porque el
   sitio es Nuxt y los datos se hidratan por JS).
3. Extrae los campos con tres estrategias en cascada:
   a. estado `__NUXT__` si existe (lo más limpio),
   b. pares `<dt>/<dd>`,
   c. heurística label→valor por clase CSS.
4. Normaliza:
   - **Date** a `YYYY-MM-DD` (reconoce "Siglo XVIII", "ca. 1680",
     rangos "1650–1700", etc.).
   - Campos multi-valor (Medium, Descriptors, etc.) como
     `"a, b, c"`.
5. Guarda un `backup.csv` local y hace `append_rows` al sheet de salida.

## 4. Si algo no funciona — modo debug

La estructura HTML exacta de ARCA puede tener selectores que yo no
verifiqué desde mi sandbox. Si ves filas vacías o datos en columnas
equivocadas:

```bash
python arca_scraper.py --debug
```

Eso crea `./debug/<id>.html` (HTML renderizado completo) y
`./debug/<id>.raw.json` (diccionario crudo extraído antes del mapeo).
Abre uno, identifica el label real (`titulo` vs `Título` vs
`tituloObra`, etc.) y añádelo al diccionario `FIELD_MAP` arriba en el
script — con eso sola ya debería quedar.


## 5. Estructura de archivos

```
.
├── arca_scraper.py      # script principal
├── requirements.txt
├── credentials.json     # tú lo añades (NO commitear)
├── backup.csv           # se genera en cada corrida
└── debug/               # solo si corres con --debug
    ├── 8548.html
    └── 8548.raw.json
```

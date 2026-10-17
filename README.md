# Pipeline ETL – SuperMart Colombia
**EAN TechRetail Solutions** · Proyecto: ETL – Pipeline de Datos para Análisis de Ventas de Retail

Pipeline en Python (pandas, numpy, matplotlib, seaborn) que extrae 3 CSV, los limpia y transforma,
carga tres salidas optimizadas para análisis y genera cuatro visualizaciones.

## Estructura
```
proyecto_etl/
├── generar_datos.py            # Crea los CSV de entrada en data/raw/
├── etl_pipeline.py             # Pipeline completo (E-T-L + gráficos)
├── requirements.txt
├── data/
│   ├── raw/                    # Entradas: ventas_crudas, productos, tiendas
│   └── processed/              # Salidas: dataset, data mart, resumen, indicadores de calidad
├── output/graficos/            # 4 visualizaciones PNG
└── logs/etl.log                # Bitácora de ejecución
```

## Ejecución
Requiere Python 3.8+ (probado con pandas 3.x).
```bash
pip install -r requirements.txt
python generar_datos.py      # opcional: regenera los CSV de entrada
python etl_pipeline.py       # ejecuta el pipeline completo
```
Rutas personalizadas: `python etl_pipeline.py --raw data/raw --out data/processed --graficos output/graficos`

## Fases
| Fase | Qué hace |
|------|----------|
| **E** | Lee los 3 CSV, valida existencia, formato y columnas; imprime registros por archivo |
| **T** | Corrige fechas (`YYYY/MM/DD`→`YYYY-MM-DD`), imputa `cliente_id` nulo con `CLIENTE_DESCONOCIDO`, elimina `order_id` duplicados, valida rangos (cantidad/precio > 0, fecha no futura), verifica integridad referencial, calcula `venta_total` y `categoria_venta` (Baja < 20, Media 20–<50, Alta ≥ 50), une productos y tiendas, crea `anio`, `mes`, `dia_semana` |
| **L** | Guarda `ventas_transformadas.csv`, `data_mart_ventas.csv` (grano: fecha × tienda × categoría), `resumen_ejecutivo.csv` (KPIs y cortes por ciudad, categoría, día, tamaño de venta y mes) e `indicadores_calidad.json` |
| **A** | Ventas por ciudad (barras), ventas por categoría (pie), transacciones por día (barras), histograma de montos |

## Anomalías intencionales en los datos de entrada
Para demostrar el control de calidad se incluyeron: fechas con `/`, `cliente_id` nulos, un `order_id`
duplicado (1003), un producto inexistente (`P000`, orden 1068) y una cantidad negativa (orden 1069).
El pipeline las detecta, las registra en el log y descarta los registros inválidos.

## Nota
El reporte ejecutivo (TXT/PDF) se entrega por separado. `indicadores_calidad.json` y
`resumen_ejecutivo.csv` contienen los insumos necesarios para elaborarlo.

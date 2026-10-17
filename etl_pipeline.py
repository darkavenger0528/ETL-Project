"""
Pipeline ETL - SuperMart Colombia | EAN TechRetail Solutions
============================================================
Fases:
  E: Extracción  -> lectura de 3 CSV con manejo de errores
  T: Transformación -> limpieza, métricas, enriquecimiento, control de calidad
  L: Carga -> dataset transformado, data mart y resumen agregado (CSV)
  A: Análisis -> 4 visualizaciones (PNG)

Uso:
    python etl_pipeline.py
    python etl_pipeline.py --raw data/raw --out data/processed --graficos output/graficos
"""
import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # backend sin interfaz: funciona en scripts y servidores
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# ------------------------------------------------------------------ #
# Configuración
# ------------------------------------------------------------------ #
COLUMNAS_ESPERADAS = {
    "ventas_crudas.csv": ["order_id", "fecha", "producto_id", "cantidad",
                          "precio_unitario", "cliente_id", "tienda_id"],
    "productos.csv": ["producto_id", "categoria", "subcategoria", "marca"],
    "tiendas.csv": ["tienda_id", "ciudad", "region", "area_m2", "gerente"],
}
DIAS_ES = {0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
           4: "Viernes", 5: "Sábado", 6: "Domingo"}
ORDEN_DIAS = list(DIAS_ES.values())
CLIENTE_DESCONOCIDO = "CLIENTE_DESCONOCIDO"

logger = logging.getLogger("etl")


def configurar_logging(ruta_log: Path) -> None:
    ruta_log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout),
                  logging.FileHandler(ruta_log, mode="w", encoding="utf-8")],
    )


# ------------------------------------------------------------------ #
# FASE 1: EXTRACCIÓN
# ------------------------------------------------------------------ #
def extraer_csv(ruta: Path, columnas_esperadas: list) -> pd.DataFrame:
    """Lee un CSV y valida existencia, contenido y estructura."""
    try:
        df = pd.read_csv(ruta, encoding="utf-8")
    except FileNotFoundError:
        raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
    except pd.errors.EmptyDataError:
        raise ValueError(f"El archivo está vacío: {ruta}")
    except pd.errors.ParserError as e:
        raise ValueError(f"Formato CSV incorrecto en {ruta}: {e}")

    faltantes = set(columnas_esperadas) - set(df.columns)
    if faltantes:
        raise ValueError(f"{ruta.name}: faltan columnas {sorted(faltantes)}")
    logger.info("Cargado %-18s -> %4d registros, %d columnas",
                ruta.name, len(df), df.shape[1])
    return df[columnas_esperadas]


def extraer_datos(dir_raw: Path) -> dict:
    logger.info("=== FASE 1: EXTRACCIÓN ===")
    datos = {}
    for archivo, columnas in COLUMNAS_ESPERADAS.items():
        datos[archivo.replace(".csv", "")] = extraer_csv(dir_raw / archivo, columnas)
    return datos


# ------------------------------------------------------------------ #
# FASE 2: TRANSFORMACIÓN
# ------------------------------------------------------------------ #
def limpiar_ventas(ventas: pd.DataFrame, calidad: dict) -> pd.DataFrame:
    """Limpieza: texto, fechas, nulos y duplicados."""
    df = ventas.copy()
    calidad["registros_iniciales"] = len(df)

    # Normalizar texto
    for col in ["producto_id", "cliente_id", "tienda_id"]:
        df[col] = df[col].astype("string").str.strip()

    # Fechas inconsistentes (YYYY/MM/DD -> YYYY-MM-DD)
    formato_inconsistente = df["fecha"].astype(str).str.contains("/").sum()
    df["fecha"] = pd.to_datetime(
        df["fecha"].astype(str).str.strip().str.replace("/", "-", regex=False),
        format="%Y-%m-%d", errors="coerce")
    calidad["fechas_formato_corregido"] = int(formato_inconsistente)
    invalidas = df["fecha"].isna().sum()
    if invalidas:
        logger.warning("Se eliminan %d registros con fecha inválida", invalidas)
        df = df.dropna(subset=["fecha"])
    calidad["fechas_invalidas_eliminadas"] = int(invalidas)

    # Nulos en cliente_id
    nulos = df["cliente_id"].isna() | (df["cliente_id"] == "")
    df.loc[nulos, "cliente_id"] = CLIENTE_DESCONOCIDO
    calidad["cliente_id_nulos_imputados"] = int(nulos.sum())
    logger.info("cliente_id nulos imputados: %d", nulos.sum())

    # IDs duplicados (se conserva la primera ocurrencia)
    dups = df.duplicated(subset="order_id", keep="first").sum()
    df = df.drop_duplicates(subset="order_id", keep="first")
    calidad["order_id_duplicados_eliminados"] = int(dups)
    logger.info("order_id duplicados eliminados: %d", dups)
    assert df["order_id"].is_unique, "Persisten order_id duplicados"
    return df


def validar_rangos(df: pd.DataFrame, calidad: dict) -> pd.DataFrame:
    """Valida cantidades/precios positivos y fechas no futuras."""
    hoy = pd.Timestamp(datetime.now().date())
    validos = (df["cantidad"] > 0) & (df["precio_unitario"] > 0) & (df["fecha"] <= hoy)
    calidad["registros_fuera_de_rango_eliminados"] = int((~validos).sum())
    if (~validos).any():
        logger.warning("Se eliminan %d registros fuera de rango (order_id: %s)",
                       (~validos).sum(), df.loc[~validos, "order_id"].tolist())
    return df[validos]


def validar_integridad_referencial(df, productos, tiendas, calidad) -> pd.DataFrame:
    """Elimina ventas cuyo producto o tienda no existe en las dimensiones."""
    prod_ok = df["producto_id"].isin(productos["producto_id"])
    tienda_ok = df["tienda_id"].isin(tiendas["tienda_id"])
    calidad["huerfanos_producto_eliminados"] = int((~prod_ok).sum())
    calidad["huerfanos_tienda_eliminados"] = int((~tienda_ok).sum())
    if not prod_ok.all():
        logger.warning("producto_id sin match: %s",
                       df.loc[~prod_ok, "producto_id"].unique().tolist())
    if not tienda_ok.all():
        logger.warning("tienda_id sin match: %s",
                       df.loc[~tienda_ok, "tienda_id"].unique().tolist())
    return df[prod_ok & tienda_ok]


def calcular_metricas(df: pd.DataFrame) -> pd.DataFrame:
    """venta_total y categoría de venta (Baja <20, Media 20-50, Alta >=50)."""
    df = df.copy()
    df["venta_total"] = (df["cantidad"] * df["precio_unitario"]).round(2)
    df["categoria_venta"] = pd.cut(
        df["venta_total"], bins=[-np.inf, 20, 50, np.inf],
        labels=["Baja", "Media", "Alta"], right=False)
    return df


def enriquecer(df, productos, tiendas) -> pd.DataFrame:
    """Join con dimensiones + dimensiones temporales."""
    df = (df.merge(productos, on="producto_id", how="left", validate="m:1")
            .merge(tiendas, on="tienda_id", how="left", validate="m:1"))
    df["anio"] = df["fecha"].dt.year
    df["mes"] = df["fecha"].dt.month
    df["dia_semana"] = df["fecha"].dt.dayofweek.map(DIAS_ES)
    return df


def transformar(datos: dict):
    logger.info("=== FASE 2: TRANSFORMACIÓN ===")
    calidad = {}
    productos, tiendas = datos["productos"], datos["tiendas"]

    for nombre, dim, pk in [("productos", productos, "producto_id"),
                            ("tiendas", tiendas, "tienda_id")]:
        if dim[pk].duplicated().any():
            raise ValueError(f"IDs duplicados en dimensión {nombre}")

    df = limpiar_ventas(datos["ventas_crudas"], calidad)
    df = validar_rangos(df, calidad)
    df = validar_integridad_referencial(df, productos, tiendas, calidad)
    df = calcular_metricas(df)
    df = enriquecer(df, productos, tiendas)

    # Indicadores finales de calidad
    calidad["registros_finales"] = len(df)
    calidad["registros_descartados"] = calidad["registros_iniciales"] - len(df)
    calidad["porcentaje_retencion"] = round(100 * len(df) / calidad["registros_iniciales"], 2)
    calidad["nulos_restantes"] = int(df.isna().sum().sum())
    calidad["integridad_referencial_ok"] = bool(
        df["categoria"].notna().all() and df["ciudad"].notna().all())
    logger.info("Registros: %d iniciales -> %d finales (%.1f%% retención)",
                calidad["registros_iniciales"], len(df), calidad["porcentaje_retencion"])

    columnas = ["order_id", "fecha", "anio", "mes", "dia_semana", "producto_id",
                "categoria", "subcategoria", "marca", "cantidad", "precio_unitario",
                "venta_total", "categoria_venta", "cliente_id", "tienda_id",
                "ciudad", "region", "area_m2", "gerente"]
    return df[columnas].sort_values("order_id").reset_index(drop=True), calidad


# ------------------------------------------------------------------ #
# FASE 3: CARGA
# ------------------------------------------------------------------ #
def construir_data_mart(df: pd.DataFrame) -> pd.DataFrame:
    """Data mart: grano = fecha x tienda x categoría x categoría de venta."""
    dm = (df.groupby(["fecha", "anio", "mes", "dia_semana", "tienda_id", "ciudad",
                      "region", "categoria"], observed=True)
            .agg(transacciones=("order_id", "nunique"),
                 unidades=("cantidad", "sum"),
                 ventas_totales=("venta_total", "sum"),
                 ticket_promedio=("venta_total", "mean"),
                 clientes_unicos=("cliente_id", "nunique"))
            .round(2).reset_index())
    return dm


def construir_resumen(df: pd.DataFrame) -> pd.DataFrame:
    """Resumen agregado en formato largo: KPIs + cortes por dimensión."""
    total = df["venta_total"].sum()
    filas = [
        ("KPI", "ventas_totales", total, len(df), df["venta_total"].mean(), 100.0),
        ("KPI", "unidades_vendidas", df["cantidad"].sum(), len(df), np.nan, np.nan),
        ("KPI", "clientes_unicos_identificados",
         df.loc[df["cliente_id"] != CLIENTE_DESCONOCIDO, "cliente_id"].nunique(),
         np.nan, np.nan, np.nan),
    ]
    resumen = pd.DataFrame(filas, columns=["tipo", "dimension", "ventas_totales",
                                           "transacciones", "ticket_promedio",
                                           "participacion_pct"])
    cortes = []
    for dim in ["ciudad", "categoria", "dia_semana", "categoria_venta", "mes"]:
        g = (df.groupby(dim, observed=True)
               .agg(ventas_totales=("venta_total", "sum"),
                    transacciones=("order_id", "count"),
                    ticket_promedio=("venta_total", "mean"))
               .reset_index().rename(columns={dim: "dimension"}))
        g["dimension"] = g["dimension"].astype(str)
        g["tipo"] = f"POR_{dim.upper()}"
        g["participacion_pct"] = 100 * g["ventas_totales"] / total
        cortes.append(g.sort_values("ventas_totales", ascending=False))
    return pd.concat([resumen] + cortes, ignore_index=True).round(2)


def cargar(df, calidad, dir_out: Path) -> None:
    logger.info("=== FASE 3: CARGA ===")
    dir_out.mkdir(parents=True, exist_ok=True)
    salidas = {
        "ventas_transformadas.csv": df,
        "data_mart_ventas.csv": construir_data_mart(df),
        "resumen_ejecutivo.csv": construir_resumen(df),
    }
    for nombre, tabla in salidas.items():
        tabla.to_csv(dir_out / nombre, index=False, encoding="utf-8-sig")
        logger.info("Guardado %-26s (%d filas)", nombre, len(tabla))
    # Indicadores de calidad (insumo para el reporte ejecutivo posterior)
    with open(dir_out / "indicadores_calidad.json", "w", encoding="utf-8") as f:
        json.dump(calidad, f, indent=2, ensure_ascii=False)
    logger.info("Guardado indicadores_calidad.json")


# ------------------------------------------------------------------ #
# FASE 4: VISUALIZACIONES
# ------------------------------------------------------------------ #
def graficar(df: pd.DataFrame, dir_graficos: Path) -> None:
    logger.info("=== FASE 4: VISUALIZACIONES ===")
    dir_graficos.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", palette="viridis")

    def guardar(nombre):
        plt.tight_layout()
        plt.savefig(dir_graficos / nombre, dpi=150)
        plt.close()
        logger.info("Gráfico guardado: %s", nombre)

    # 1. Ventas totales por ciudad
    por_ciudad = df.groupby("ciudad")["venta_total"].sum().sort_values(ascending=False)
    plt.figure(figsize=(9, 5))
    ax = sns.barplot(x=por_ciudad.index, y=por_ciudad.values, hue=por_ciudad.index,
                     legend=False)
    for c in ax.containers:
        ax.bar_label(c, fmt="$%.0f", padding=3)
    plt.title("Ventas totales por ciudad", fontweight="bold")
    plt.xlabel("Ciudad"); plt.ylabel("Ventas totales ($)")
    guardar("01_ventas_por_ciudad.png")

    # 2. Distribución de ventas por categoría (pie)
    por_cat = df.groupby("categoria")["venta_total"].sum().sort_values(ascending=False)
    plt.figure(figsize=(7, 7))
    plt.pie(por_cat.values, labels=por_cat.index, autopct="%1.1f%%", startangle=90,
            colors=sns.color_palette("viridis", len(por_cat)),
            wedgeprops={"edgecolor": "white"})
    plt.title("Distribución de ventas por categoría", fontweight="bold")
    guardar("02_ventas_por_categoria.png")

    # 3. Transacciones por día de la semana
    por_dia = (df.groupby("dia_semana")["order_id"].count()
                 .reindex(ORDEN_DIAS, fill_value=0))
    plt.figure(figsize=(9, 5))
    ax = sns.barplot(x=por_dia.index, y=por_dia.values, hue=por_dia.index, legend=False)
    for c in ax.containers:
        ax.bar_label(c, padding=3)
    plt.title("Transacciones por día de la semana", fontweight="bold")
    plt.xlabel("Día"); plt.ylabel("N.º de transacciones")
    guardar("03_transacciones_por_dia.png")

    # 4. Histograma de montos de venta
    plt.figure(figsize=(9, 5))
    sns.histplot(df["venta_total"], bins=15, kde=True, color="#2a788e")
    plt.axvline(df["venta_total"].mean(), color="red", ls="--",
                label=f"Media: ${df['venta_total'].mean():.2f}")
    plt.axvline(df["venta_total"].median(), color="orange", ls="--",
                label=f"Mediana: ${df['venta_total'].median():.2f}")
    plt.title("Distribución de montos de venta", fontweight="bold")
    plt.xlabel("Monto de venta ($)"); plt.ylabel("Frecuencia"); plt.legend()
    guardar("04_histograma_montos.png")


# ------------------------------------------------------------------ #
# MAIN
# ------------------------------------------------------------------ #
def ejecutar_pipeline(dir_raw: Path, dir_out: Path, dir_graficos: Path) -> pd.DataFrame:
    datos = extraer_datos(dir_raw)
    df, calidad = transformar(datos)
    cargar(df, calidad, dir_out)
    graficar(df, dir_graficos)
    logger.info("Pipeline finalizado correctamente")
    return df


def main():
    p = argparse.ArgumentParser(description="Pipeline ETL SuperMart Colombia")
    p.add_argument("--raw", type=Path, default=Path("data/raw"))
    p.add_argument("--out", type=Path, default=Path("data/processed"))
    p.add_argument("--graficos", type=Path, default=Path("output/graficos"))
    p.add_argument("--log", type=Path, default=Path("logs/etl.log"))
    args = p.parse_args()

    configurar_logging(args.log)
    try:
        ejecutar_pipeline(args.raw, args.out, args.graficos)
    except (FileNotFoundError, ValueError) as e:
        logger.error("Error en el pipeline: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error inesperado")
        sys.exit(2)


if __name__ == "__main__":
    main()

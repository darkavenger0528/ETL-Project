"""
Genera los tres archivos CSV de entrada (data/raw/).

Incluye los registros base del enunciado + registros adicionales y
algunas anomalías intencionales para demostrar la limpieza del pipeline:
  - formatos de fecha mixtos (YYYY-MM-DD y YYYY/MM/DD)
  - cliente_id nulo
  - order_id duplicado, producto inexistente y cantidad negativa
"""
from pathlib import Path
import numpy as np
import pandas as pd

RAW = Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)
rng = np.random.default_rng(42)

# ---------------- productos.csv ----------------
productos = pd.DataFrame([
    ("P123", "Electrónicos", "Smartphones", "Samsung"),
    ("P456", "Hogar", "Electrodomésticos", "LG"),
    ("P789", "Alimentos", "Bebidas", "Coca-Cola"),
    ("P321", "Ropa", "Calzado", "Nike"),
    ("P999", "Electrónicos", "Tablets", "Apple"),
    ("P101", "Alimentos", "Lácteos", "Alpina"),
    ("P102", "Alimentos", "Snacks", "Frito-Lay"),
    ("P103", "Hogar", "Muebles", "IKEA"),
    ("P104", "Ropa", "Camisetas", "Adidas"),
    ("P105", "Electrónicos", "Audio", "Sony"),
], columns=["producto_id", "categoria", "subcategoria", "marca"])

# ---------------- tiendas.csv ----------------
tiendas = pd.DataFrame([
    ("T001", "Bogotá", "Cundinamarca", 500, "María González"),
    ("T002", "Medellín", "Antioquia", 450, "Carlos Rodríguez"),
    ("T003", "Cali", "Valle", 400, "Ana Martínez"),
    ("T004", "Barranquilla", "Atlántico", 350, "Pedro López"),
    ("T005", "Cartagena", "Bolívar", 300, "Laura Ramírez"),
], columns=["tienda_id", "ciudad", "region", "area_m2", "gerente"])

# ---------------- ventas_crudas.csv ----------------
# Registros base del enunciado (tal cual, con sus inconsistencias)
base = [
    (1001, "2024-03-15", "P123", 2, 15.50, "C001", "T005"),
    (1002, "2024-03-15", "P456", 1, 25.00, "C002", "T003"),
    (1003, "2024/03/16", "P789", 3, 10.00, None, "T001"),
    (1004, "2024-03-16", "P123", 1, 15.50, "C003", "T002"),
    (1005, "2024-03-17", "P999", 2, 30.00, "C004", "T004"),
    (1006, "2024-03-17", "P321", 1, 45.00, "C005", "T001"),
    (1007, "2024-03-18", "P456", 2, 25.00, "C001", "T003"),
]

precios = {"P123": 15.50, "P456": 25.00, "P789": 10.00, "P321": 45.00,
           "P999": 30.00, "P101": 4.50, "P102": 3.20, "P103": 120.00,
           "P104": 28.00, "P105": 85.00}

extra = []
fechas = pd.date_range("2024-03-18", "2024-04-14")
for oid in range(1008, 1068):
    pid = rng.choice(list(precios))
    fecha = rng.choice(fechas)
    fecha_str = pd.Timestamp(fecha).strftime("%Y-%m-%d")
    if rng.random() < 0.15:                      # formato inconsistente
        fecha_str = fecha_str.replace("-", "/")
    cliente = f"C{rng.integers(1, 16):03d}" if rng.random() > 0.08 else None
    extra.append((oid, fecha_str, pid, int(rng.integers(1, 6)), precios[pid],
                  cliente, f"T{rng.integers(1, 6):03d}"))

# Anomalías intencionales para probar el control de calidad
anomalias = [
    (1003, "2024/03/16", "P789", 3, 10.00, None, "T001"),   # order_id duplicado
    (1068, "2024-04-02", "P000", 1, 20.00, "C006", "T002"), # producto inexistente
    (1069, "2024-04-03", "P123", -1, 15.50, "C007", "T003"),# cantidad negativa
]

cols = ["order_id", "fecha", "producto_id", "cantidad",
        "precio_unitario", "cliente_id", "tienda_id"]
ventas = pd.DataFrame(base + extra + anomalias, columns=cols)

ventas.to_csv(RAW / "ventas_crudas.csv", index=False)
productos.to_csv(RAW / "productos.csv", index=False)
tiendas.to_csv(RAW / "tiendas.csv", index=False)
print(f"Generados: ventas={len(ventas)}, productos={len(productos)}, tiendas={len(tiendas)}")

import os
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# ========= CONFIG =========

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "booking_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "booking_password")
DB_NAME = os.getenv("DB_NAME", "booking_db")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

app = FastAPI(title="Booking Analytics API")

# Разрешаем запросы с фронта
origins = [
    "http://localhost:8081",
    "http://127.0.0.1:8081",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========= Pydantic-схемы =========

class RFMSummary(BaseModel):
    customers_total: int
    avg_recency: float
    avg_frequency: float
    avg_monetary: float


class RFMClassItem(BaseModel):
    rfm_class: str
    customers_count: int
    avg_monetary: float


class RFMTopCustomer(BaseModel):
    guest_id: int
    recency: int
    frequency: int
    monetary: float
    rfm_class: str


# ========= ЭНДПОИНТЫ =========

@app.get("/health", tags=["system"])
def health():
    return {"status": "ok"}


@app.get("/api/rfm/summary", response_model=RFMSummary, tags=["rfm"])
def get_rfm_summary():
    """
    Общие агрегаты по RFM-таблице.
    """
    query = text("""
        SELECT
            COUNT(*)::int                        AS customers_total,
            AVG(recency)::float                  AS avg_recency,
            AVG(frequency)::float                AS avg_frequency,
            AVG(monetary)::float                 AS avg_monetary
        FROM bookings_rfm;
    """)

    with engine.connect() as conn:
        row = conn.execute(query).mappings().one()

    return RFMSummary(**row)


@app.get("/api/rfm/class-distribution",
         response_model=List[RFMClassItem],
         tags=["rfm"])
def get_rfm_class_distribution():
    """
    Распределение клиентов по RFM-классам + средний Monetary в каждом классе.
    """
    query = text("""
        SELECT
            rfm_class,
            COUNT(*)::int            AS customers_count,
            AVG(monetary)::float     AS avg_monetary
        FROM bookings_rfm
        GROUP BY rfm_class
        ORDER BY rfm_class;
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    return [RFMClassItem(**row) for row in rows]


@app.get("/api/rfm/top-customers",
         response_model=List[RFMTopCustomer],
         tags=["rfm"])
def get_top_customers(limit: int = 20):
    """
    Топ клиентов по Monetary.
    """
    query = text("""
        SELECT
            guest_id,
            recency,
            frequency,
            monetary,
            rfm_class
        FROM bookings_rfm
        ORDER BY monetary DESC
        LIMIT :limit;
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit}).mappings().all()

    return [RFMTopCustomer(**row) for row in rows]

from datetime import datetime, timedelta
import logging

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ========= КОНФИГ =========

POSTGRES_CONN_ID = "booking_db"
SOURCE_TABLE = "bookings"
RFM_TABLE = "bookings_rfm"

logger = logging.getLogger(__name__)

# Маппинг типа номера → "ценность" ночи (как в ноутбуке)
ROOM_RANK = {
    "СТД-СТАНДАРТ": 1,
    "СТД 2 - СТД2-СТАНДАРТ(2-222, 2-227)": 2,
    "СТДКОМФОРТ": 3,
    "СТДVS - VS-СТАНДАРТ": 4,
    "К -Комфорт": 5,
    "К-VS - VS-Комфорт": 6,
    "КфМрс - Комфорт Морской": 6,
    "К+- Комфорт+": 7,
    "К+VS - VS-Комфорт+": 8,
    "МРСК - Морская прохлада (К+)": 9,
    "ЛЮКС": 10,
    "ЛК2-ЛЮКС 2 КОМНАТЫ": 11,
    "ЛК2VS - VS-ЛЮКС 2 КОМНАТНЫЙ": 12,
    "Брлн-Берлин": 12,
    "ЛК3-ЛЮКС 3 КОМНАТЫ": 13,
    "ЛК3VS - VS-ЛЮКС 3 КОМНАТНЫЙ": 14,
    "АПАРТ-АПАРТАМЕНТЫ": 15,
    "АП+ - АПАРТ ПЛЮС 1-НО КОМНАТНЫЙ": 16,
    "АП2К - АПАРТ 2 -Х КОМНАТНЫЙ": 17,
    "АП+2К - АПАРТ+ 2 -Х КОМНАТНЫЙ": 18,
    "АП3К - АПАРТ 3-Х КОМНАТНЫЙ": 19,
    "АП+3К - 3 -Х КОМНАТНЫЙ": 20,
    "АПVS - VS-АПАРТАМЕНТЫ": 21,
}

EXCLUDE_TARIFFS = ["Служебный (с питанием)", "Служебный (без питания)"]
EXCLUDE_ROOM_TYPE = "СЛУЖБ-СЛУЖЕБНЫЙ"


# ========= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =========

def r_class(x, p, d):
    """Скорая для Recency: чем меньше, тем лучше (1 — лучший)."""
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]:
        return 3
    else:
        return 4


def fm_class(x, p, d):
    """Скорая для Frequency/Monetary: чем больше, тем лучше (1 — лучший)."""
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.50]:
        return 3
    elif x <= d[p][0.75]:
        return 2
    else:
        return 1


# ========= TASK: RFM-РАСЧЁТ И ЗАПИСЬ =========

def calculate_and_load_rfm():
    """
    1) Читает bookings из Postgres
    2) Считает RFM
    3) Перезаписывает таблицу bookings_rfm
    """
    context = get_current_context()
    logical_date = context["logical_date"]
    logger.info("Старт RFM-расчёта, logical_date=%s", logical_date)

    # 1. Читаем данные из Postgres
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    query = f"""
        SELECT
            guest_id,
            check_in_ts,
            check_out_ts,
            room_type,
            tariff
        FROM {SOURCE_TABLE}
        WHERE guest_id IS NOT NULL
          AND check_in_ts IS NOT NULL
          AND check_out_ts IS NOT NULL
    """

    df = pd.read_sql(query, engine)
    logger.info("Загружено %d строк из %s", len(df), SOURCE_TABLE)

    if df.empty:
        logger.warning("Данных в %s нет, RFM не рассчитан", SOURCE_TABLE)
        return

    # 2. Бизнес-очистка
    df["check_in_ts"] = pd.to_datetime(df["check_in_ts"])
    df["check_out_ts"] = pd.to_datetime(df["check_out_ts"])

    # nights = (дата выезда - дата заезда) в днях
    df["nights"] = (
        df["check_out_ts"].dt.normalize() - df["check_in_ts"].dt.normalize()
    ).dt.days

    # Фильтры по ночам/типу номера/тарифам
    df = df[
        (df["nights"] > 0)
        & (df["room_type"] != EXCLUDE_ROOM_TYPE)
        & (~df["tariff"].isin(EXCLUDE_TARIFFS))
    ]
    logger.info("После фильтров осталось %d строк", len(df))

    if df.empty:
        logger.warning("После бизнес-фильтров нет данных для RFM")
        return

    # NOW — дата расчёта Recency = logical_date DAG’а (делаем naive, без tz)
    now_ts = pd.Timestamp(logical_date).tz_localize(None).normalize()
    logger.info("NOW для Recency (logical_date, naive): %s", now_ts)

    # Отбрасываем брони, которые ещё не начались (чисто будущее)
    df = df[df["check_in_ts"] <= now_ts]
    logger.info("После отсечения будущих заездов осталось %d строк", len(df))

    if df.empty:
        logger.warning("Все заезды в будущем относительно %s, RFM не рассчитан", now_ts)
        return

    # 3. Вес ночи по типу номера
    df["room_rank"] = df["room_type"].map(ROOM_RANK).fillna(1)
    df["weighted_nights"] = df["nights"] * df["room_rank"]

    # 4. RFM-агрегация (snake_case)
    tmp = df.groupby("guest_id").agg(
        last_date=("check_out_ts", "max"),
        frequency=("guest_id", "size"),
        monetary=("weighted_nights", "sum"),
    )
    tmp["recency"] = (now_ts - tmp["last_date"]).dt.days.clip(lower=0)
    rfm = tmp.drop(columns="last_date").reset_index()

    logger.info(
        "Сформирована RFM-таблица: %d строк, колонки: %s",
        len(rfm),
        list(rfm.columns),
    )

    # 5. Квартильные границы
    quantiles = rfm[["recency", "frequency", "monetary"]].quantile(
        q=[0.25, 0.5, 0.75]
    )
    quantiles = quantiles.to_dict()

    # 6. R/F/M quartile scores
    rfm["r_quartile"] = rfm["recency"].apply(r_class, args=("recency", quantiles))
    rfm["f_quartile"] = rfm["frequency"].apply(
        fm_class, args=("frequency", quantiles)
    )
    rfm["m_quartile"] = rfm["monetary"].apply(
        fm_class, args=("monetary", quantiles)
    )

    # 7. RFM-класс вида "241"
    rfm["rfm_class"] = (
        rfm["r_quartile"].map(str)
        + rfm["f_quartile"].map(str)
        + rfm["m_quartile"].map(str)
    )

    # 8. Техническое поле
    rfm["calculated_at"] = pd.Timestamp.utcnow()

    # 9. Перезаписываем таблицу bookings_rfm
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE TABLE {RFM_TABLE};")

    rfm.to_sql(
        name=RFM_TABLE,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10_000,
    )

    logger.info("✓ В таблицу %s записано %d строк", RFM_TABLE, len(rfm))


# ========= DAG =========

default_args = {
    "owner": "data_analyst",
    "depends_on_past": False,
    "email": ["admin@example.com"],   # можно убрать и email_on_failure=False, если не нужен SMTP
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="booking_rfm_pipeline",
    default_args=default_args,
    description="RFM-анализ клиентов на основе таблицы bookings → bookings_rfm",
    schedule_interval="@daily",
    catchup=False,
    tags=["booking", "rfm", "analytics"],
) as dag:

    # 1. Создаём таблицу RFM (с нуля, под snake_case)
    create_rfm_table = PostgresOperator(
        task_id="create_rfm_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
        DROP TABLE IF EXISTS {RFM_TABLE};
        CREATE TABLE {RFM_TABLE} (
            id            SERIAL PRIMARY KEY,
            guest_id      BIGINT NOT NULL,

            recency       INTEGER,
            frequency     INTEGER,
            monetary      NUMERIC(18,2),

            r_quartile    INTEGER,
            f_quartile    INTEGER,
            m_quartile    INTEGER,
            rfm_class     VARCHAR(3),

            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # 2. На всякий случай чистим перед загрузкой (в будущем можно убрать)
    truncate_rfm_table = PostgresOperator(
        task_id="truncate_rfm_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"TRUNCATE TABLE {RFM_TABLE};",
    )

    # 3. Расчёт и заливка RFM
    calculate_rfm = PythonOperator(
        task_id="calculate_and_load_rfm",
        python_callable=calculate_and_load_rfm,
    )

    create_rfm_table >> truncate_rfm_table >> calculate_rfm

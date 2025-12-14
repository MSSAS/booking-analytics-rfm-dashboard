from datetime import datetime, timedelta
import os
import logging
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import pandas as pd

# ============ КОНФИГУРАЦИЯ ============

POSTGRES_CONN_ID = 'booking_db'  # ID подключения в Airflow
TABLE_NAME = "bookings"          # Новая таблица

YANDEX_DISK_URL = "https://disk.yandex.ru/d/64FMcA3te7mA9A"

# Маппинг исходных колонок → поля в таблице bookings
COLUMN_RENAME_MAP = {
    'Дата создания': 'created_date',
    'Заезд': 'check_in_ts',
    'Выезд': 'check_out_ts',
    'Тариф': 'tariff',
    'Скидка': 'discount',
    'Состав': 'composition',
    'Группа': 'group_code',
    'Тип номера': 'room_type',
    'Валюта': 'currency',
    'Гость': 'guest_id',          # ID гостя
    'Гость.1': 'guest_name',      # Имя/фамилия
    'Гражданство': 'citizenship',
    'Пол': 'gender',
    'Гость.2': 'guest_age',
    'Пред. заезды': 'previous_stays',
    'Пр.': 'nights',
    'Номеров': 'rooms_count',
    'Мест': 'seats_count',
    'Доп. мест': 'extra_seats',
    'Подр.': 'details',
    'Детей': 'children',
    'Взр.': 'adults',
}

logger = logging.getLogger(__name__)

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def clean_int_series(s: pd.Series) -> pd.Series:
    """
    Чистим числовую колонку от пробелов, запятых, .00 и приводим к Int64.
    Применяем для guest_id, group_code и похожих.
    """
    return (
        pd.to_numeric(
            s.astype(str)
             .str.replace(' ', '', regex=False)
             .str.replace(',', '', regex=False)
             .str.replace('.00', '', regex=False),
            errors='coerce'
        ).astype('Int64')
    )


# ============ TASK: ЗАГРУЗКА И ОЧИСТКА ДАННЫХ ============

def download_and_clean_data():
    """
    Загружает файл из Yandex Disk, очищает данные, приводит типы
    и сохраняет во временный CSV. В XCom кладёт путь к файлу и кол-во строк.
    """
    try:
        context = get_current_context()
        ts_nodash = context['ts_nodash']
        ti = context['ti']

        logger.info("Начинаю загрузку данных из Yandex Disk...")

        # 1. Получаем прямую ссылку на скачивание с Яндекс.Диска
        api_url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
        params = {"public_key": YANDEX_DISK_URL}

        resp = requests.get(api_url, params=params, timeout=30)
        resp.raise_for_status()

        download_url = resp.json()["href"]
        logger.info("Получена ссылка на скачивание: %s", download_url)

        # 2. Скачиваем файл
        file_resp = requests.get(download_url, timeout=60)
        file_resp.raise_for_status()
        logger.info(
            "Файл загружен, размер: %.2f MB",
            len(file_resp.content) / (1024 * 1024),
        )

        # 3. Читаем в pandas
        df = pd.read_excel(BytesIO(file_resp.content))
        logger.info(
            "Исходный DataFrame: %d строк, %d столбцов",
            df.shape[0], df.shape[1]
        )

        # 4. Удаляем лишние/пустые вещи
        if 'Unnamed: 1' in df.columns:
            df = df.drop('Unnamed: 1', axis=1)
            logger.info("Удалён столбец 'Unnamed: 1'")

        # Важные поля: без них строка нам не нужна
        df = df.dropna(subset=['Заезд', 'Выезд', 'Гость'])
        logger.info("После удаления пустых строк: %d строк", df.shape[0])

        # 5. Переименовываем столбцы
        df = df.rename(columns=COLUMN_RENAME_MAP)
        logger.info("Столбцы после переименования: %s", list(df.columns))

        # 6. Преобразование дат
        date_columns = ['created_date', 'check_in_ts', 'check_out_ts']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

        # created_date в БД — DATE, берём только дату
        if 'created_date' in df.columns:
            df['created_date'] = df['created_date'].dt.date

        # 7. Числовые поля с очисткой (ID и group_code с пробелами/запятыми)
        for col in ['guest_id', 'group_code']:
            if col in df.columns:
                df[col] = clean_int_series(df[col])

        # Остальные целочисленные поля
        int_columns = [
            'guest_age',
            'previous_stays',
            'nights',
            'rooms_count',
            'seats_count',
            'extra_seats',
            'children',
            'adults',
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # Скидка как число с плавающей точкой
        if 'discount' in df.columns:
            df['discount'] = pd.to_numeric(df['discount'], errors='coerce')

        logger.info("Типы данных приведены к нужному формату")

        # 8. Сохраняем очищенный df во временный CSV
        file_path = f"/tmp/booking_{ts_nodash}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8')
        logger.info("Очищенный DataFrame сохранён в файл %s", file_path)

        # 9. XCom: путь к файлу и кол-во строк
        ti.xcom_push(key='file_path', value=file_path)
        ti.xcom_push(key='rows_total', value=len(df))

        logger.info("✓ Данные успешно загружены и сохранены во временный файл")
        return True

    except Exception as e:
        logger.error("✗ Ошибка при загрузке/очистке данных: %s", str(e))
        raise


# ============ TASK: ЗАПИСЬ В POSTGRES ============

def insert_data_into_postgres():
    """
    Считывает очищенные данные из CSV и вставляет в PostgreSQL (таблица bookings).
    """
    try:
        context = get_current_context()
        ti = context['ti']

        logger.info("Начинаю вставку данных в PostgreSQL...")

        # Тянем путь к файлу из XCom
        file_path = ti.xcom_pull(
            task_ids='download_and_clean',
            key='file_path',
        )

        if not file_path:
            raise ValueError(
                "Не удалось получить XCom 'file_path' "
                "из задачи 'download_and_clean'."
            )

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл с данными не найден: {file_path}")

        logger.info("Читаю CSV из %s", file_path)
        df = pd.read_csv(file_path)

        logger.info(
            "DataFrame для загрузки: %d строк, %d столбцов",
            df.shape[0], df.shape[1]
        )

        # Получаем подключение к PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        # Вставляем данные
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=10_000,
        )

        rows_inserted = len(df)
        logger.info(
            "✓ Успешно вставлено %d строк в таблицу %s",
            rows_inserted, TABLE_NAME
        )

        # Удаляем временный файл
        try:
            os.remove(file_path)
            logger.info("Временный файл %s удалён", file_path)
        except OSError as e:
            logger.warning("Не удалось удалить файл %s: %s", file_path, e)

        ti.xcom_push(key='rows_inserted', value=rows_inserted)
        return True

    except Exception as e:
        logger.error("✗ Ошибка при вставке данных в PostgreSQL: %s", str(e))
        raise


# ============ TASK: ЛОГИРОВАНИЕ ИТОГОВ ============

def log_summary():
    """
    Логирует итоговую информацию о загрузке.
    """
    context = get_current_context()
    ti = context['ti']

    rows_inserted = ti.xcom_pull(
        task_ids='insert_data',
        key='rows_inserted',
    )

    logger.info(f"""
    ╔════════════════════════════════════╗
    ║   ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО       ║
    ╠════════════════════════════════════╣
    ║ Таблица: {TABLE_NAME:<19} ║
    ║ Загружено строк: {rows_inserted:<14} ║
    ║ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<15} ║
    ╚════════════════════════════════════╝
    """)


# ============ DAG DEFINITION ============

default_args = {
    'owner': 'data_analyst',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

dag = DAG(
    'booking_data_pipeline',
    default_args=default_args,
    description='Загрузка данных бронирования из Yandex Disk в PostgreSQL (таблица bookings)',
    schedule_interval='@daily',  # Ежедневно
    catchup=False,
    tags=['booking', 'etl', 'postgres'],
)

# ============ TASKS ============

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id              SERIAL PRIMARY KEY,

        created_date    DATE,
        check_in_ts     TIMESTAMP,
        check_out_ts    TIMESTAMP,

        tariff          VARCHAR(100),
        discount        NUMERIC(10,2),

        composition     VARCHAR(255),
        group_code      INTEGER,
        room_type       VARCHAR(100),
        currency        VARCHAR(20),

        guest_id        BIGINT,
        guest_name      VARCHAR(255),
        citizenship     VARCHAR(100),
        gender          VARCHAR(10),
        guest_age       INTEGER,

        previous_stays  INTEGER,
        nights          INTEGER,
        rooms_count     INTEGER,
        seats_count     INTEGER,
        extra_seats     INTEGER,

        details         VARCHAR(255),
        children        INTEGER,
        adults          INTEGER,

        loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

download_task = PythonOperator(
    task_id='download_and_clean',
    python_callable=download_and_clean_data,
    dag=dag,
)

insert_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data_into_postgres,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='log_summary',
    python_callable=log_summary,
    dag=dag,
)

# ============ DEPENDENCIES ============

create_table_task >> download_task >> insert_task >> summary_task
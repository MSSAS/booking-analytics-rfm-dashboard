CREATE TABLE IF NOT EXISTS bookings (
    id              SERIAL PRIMARY KEY,

    created_date    DATE,            -- "Дата создания"
    check_in_ts     TIMESTAMP,       -- "Заезд"  (с временем)
    check_out_ts    TIMESTAMP,       -- "Выезд"

    tariff          VARCHAR(100),    -- "Тариф"
    discount        NUMERIC(10,2),   -- "Скидка" (процент или сумма)

    composition     VARCHAR(255),    -- "Состав"
    group_code      INTEGER,         -- "Группа" (900000 и т.п. после очистки)
    room_type       VARCHAR(100),    -- "Тип номера"
    currency        VARCHAR(20),     -- "Валюта" ("Руб." и т.п.)

    guest_id        BIGINT,          -- "Гость" (1,313,216.00 -> 1313216)
    guest_name      VARCHAR(255),    -- "Гость.1"
    citizenship     VARCHAR(100),    -- "Гражданство"
    gender          VARCHAR(10),     -- "Пол"
    guest_age       INTEGER,         -- "Гость.2"

    previous_stays  INTEGER,         -- "Пред. заезды"
    nights          INTEGER,         -- "Пр." (скорее всего количество ночей)
    rooms_count     INTEGER,         -- "Номеров"
    seats_count     INTEGER,         -- "Мест"
    extra_seats     INTEGER,         -- "Доп. мест"

    details         VARCHAR(255),    -- "Подр." (подробности / подтип)
    children        INTEGER,         -- "Детей"
    adults          INTEGER,         -- "Взр."

    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- тех. поле
);

CREATE TABLE IF NOT EXISTS bookings_rfm (
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
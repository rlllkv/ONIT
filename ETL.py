import requests
import json
import logging
import time
from datetime import datetime
from sqlalchemy import create_engine, text

# --- КОНФИГУРАЦИЯ ---
DB_USER = "trudvsem_parser"
DB_PASS = "123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "trudvsem_analytics"

API_URL = "http://opendata.trudvsem.ru/api/v1/vacancies"

# Цель: сколько всего записей хотим
TARGET_TOTAL_RECORDS = 5000 
BATCH_SIZE = 20 

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Connection': 'keep-alive'
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# --- ФУНКЦИИ ---

def safe_float(value):
    if value is None: return None
    if isinstance(value, (float, int)): return float(value)
    if isinstance(value, str):
        val = value.strip()
        if not val: return None
        try: return float(val)
        except ValueError: return None
    return None

def get_data_with_retry(offset, retries=5):
    """
    Пытается получить данные. Если 500 ошибка — ждет и пробует снова.
    """
    params = {'limit': BATCH_SIZE, 'offset': offset}
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(API_URL, params=params, headers=HEADERS, timeout=60)
            
            # Если 500-е ошибки сервера — кидаем исключение вручную, чтобы попасть в except
            if 500 <= response.status_code < 600:
                response.raise_for_status()
                
            response.raise_for_status() # Проверка остальных кодов (404, 403)
            response.encoding = 'utf-8'
            return response.json()
            
        except requests.exceptions.RequestException as e:
            wait_time = attempt * 2 # Прогрессивная задержка: 2, 4, 6, 8, 10 сек
            logger.warning(f"⚠️ Попытка {attempt}/{retries} неудачна (HTTP Error/Timeout). Ждем {wait_time}сек... Ошибка: {e}")
            time.sleep(wait_time)
            
    logger.error("❌ Все попытки исчерпаны. Пропускаем этот батч.")
    return None

def init_db(engine):
    ddl_raw = """
    CREATE TABLE IF NOT EXISTS raw_data_log (
        id SERIAL PRIMARY KEY,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        offset_val INTEGER,
        raw_json JSONB
    );
    """
    ddl_mart = """
    CREATE TABLE IF NOT EXISTS vacancies_mart (
        vacancy_uuid TEXT PRIMARY KEY,
        job_title TEXT,
        salary_min INTEGER,
        salary_max INTEGER,
        region_name TEXT,
        specialisation TEXT,
        latitude FLOAT,
        longitude FLOAT,
        date_published DATE,
        url TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl_raw))
        conn.execute(text(ddl_mart))

# --- MAIN ---

def run_etl():
    try:
        engine = create_engine(DATABASE_URI, pool_pre_ping=True)
        init_db(engine)
        logger.info("✅ Подключение к БД успешно. Таблицы готовы.")
    except Exception as e:
        logger.critical(f"🔥 Не удалось подключиться к БД: {e}")
        return

    # Подготовленные SQL-запросы
    sql_raw = text("INSERT INTO raw_data_log (loaded_at, offset_val, raw_json) VALUES (:loaded_at, :offset, :raw_json)")
    sql_mart = text("""
        INSERT INTO vacancies_mart (
            vacancy_uuid, job_title, salary_min, salary_max, region_name, 
            specialisation, latitude, longitude, date_published, url, updated_at
        ) VALUES (
            :uuid, :title, :s_min, :s_max, :reg, 
            :spec, :lat, :lon, :d_pub, :url, :now
        )
        ON CONFLICT (vacancy_uuid) DO UPDATE SET
            job_title = EXCLUDED.job_title,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            updated_at = EXCLUDED.updated_at;
    """)

    current_offset = 0
    total_saved = 0

    logger.info(f"🚀 Старт выгрузки. Цель: {TARGET_TOTAL_RECORDS} записей.")
    
    try:
        while total_saved < TARGET_TOTAL_RECORDS:
            
            # 1. EXTRACT с повторными попытками
            data = get_data_with_retry(current_offset)
            
            if data is None:
                logger.error("API недоступен. Прерываем выполнение, чтобы не спамить.")
                break

            vacancies = data.get("results", {}).get("vacancies", [])
            if not vacancies:
                logger.info("🏁 API вернул пустой список. Вакансии закончились.")
                break

            # 2. LOAD & TRANSFORM (Транзакция)
            with engine.begin() as conn:
                # А. Лог
                conn.execute(sql_raw, {
                    "loaded_at": datetime.now(),
                    "offset": current_offset,
                    "raw_json": json.dumps(data, ensure_ascii=False)
                })

                # Б. Витрина
                batch_count = 0
                for item in vacancies:
                    vac = item.get("vacancy", {})
                    if not vac.get("id"): continue
                    
                    try:
                        # Координаты
                        lat, lng = None, None
                        addrs = vac.get("addresses", {}).get("address", [])
                        if isinstance(addrs, list) and addrs and isinstance(addrs[0], dict):
                            lat = safe_float(addrs[0].get("lat"))
                            lng = safe_float(addrs[0].get("lng"))

                        dto = {
                            "uuid": vac.get("id"),
                            "title": vac.get("job-name"),
                            "s_min": safe_float(vac.get("salary_min")),
                            "s_max": safe_float(vac.get("salary_max")),
                            "reg": vac.get("region", {}).get("name") if isinstance(vac.get("region"), dict) else None,
                            "spec": vac.get("category", {}).get("specialisation") if isinstance(vac.get("category"), dict) else None,
                            "lat": lat, "lon": lng,
                            "d_pub": vac.get("creation-date"),
                            "url": vac.get("vac_url"),
                            "now": datetime.now()
                        }
                        conn.execute(sql_mart, dto)
                        batch_count += 1
                    except Exception:
                        continue # Ошибка одной строки не ломает батч
                
                total_saved += batch_count
                # === КОНЕЦ ТРАНЗАКЦИИ (ТУТ ПРОИСХОДИТ COMMIT) ===
            
            logger.info(f"💾 Батч сохранен (offset={current_offset}). Всего в базе: {total_saved}")
            
            current_offset += BATCH_SIZE
            time.sleep(0.3)

    except KeyboardInterrupt:
        logger.warning("\n🛑 Скрипт остановлен пользователем (Ctrl+C).")
        logger.warning(f"Данные, сохраненные до этого момента ({total_saved} шт), лежат в базе.")
    
    logger.info("Работа завершена.")

if __name__ == "__main__":
    run_etl()
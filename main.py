import requests
import sqlite3
import time
import random
import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple

import os

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

# ==================================================
# НАСТРОЙКИ
# ==================================================
PROFESSIONS = [
    'Data Engineer', 'Python разработчик', 'Java разработчик',
    'DevOps инженер', 'Data Scientist', 'Machine Learning Engineer',
    'Backend разработчик', 'Frontend разработчик', 'Fullstack разработчик',
    'Системный аналитик'
]

CLIENT_ID = "MUF52QUJ17OB0LH9RQQEFIV5INMT46FM2VK26NFRH9BI2JLPLRP772L4F8B1H3RQ"
CLIENT_SECRET = "K9IINSL93OEQCCHG17QCNM86PBIJ6CJ6QU8QENIUJ8OK4QMLK87K0TD617JSEV1B"
TOKEN_FILE = "hh_token.json"

AREA = 113
PER_PAGE = 50
MAX_PAGES = 5

PAUSE_VACANCY = (0.2, 0.3)
PAUSE_PROFESSION = (1.5, 2)
PAUSE_PAGE = (1, 1.5)

hh_token = None


# ==================================================
# ТОКЕН
# ==================================================
def load_token():
    global hh_token
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                data = json.load(f)
                hh_token = data.get('access_token')
                return True
        except:
            pass
    return False


def save_token(token):
    with open(TOKEN_FILE, 'w') as f:
        json.dump({'access_token': token}, f)


def get_token():
    global hh_token
    if hh_token:
        return True
    if load_token():
        return True
    try:
        resp = requests.post("https://hh.ru/oauth/token", data={
            'grant_type': 'client_credentials',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        }, timeout=10)
        if resp.status_code == 200:
            hh_token = resp.json().get('access_token')
            save_token(hh_token)
            return True
    except:
        pass
    return False


# ==================================================
# БАЗА ДАННЫХ
# ==================================================
def init_db() -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    conn = sqlite3.connect('hh_vacancies.db')
    cur = conn.cursor()

    # Основная таблица вакансий с полями для отслеживания жизни
    cur.execute("""CREATE TABLE IF NOT EXISTS vacancies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        profession TEXT,
        title TEXT,
        company TEXT,
        city TEXT,
        salary_from INTEGER,
        salary_to INTEGER,
        salary_currency TEXT,
        key_skills TEXT,
        url TEXT UNIQUE,
        published_at TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active INTEGER DEFAULT 1
    )""")

    # Индексы
    for idx in ['profession', 'published_at', 'salary_from', 'salary_to', 'last_seen', 'is_active']:
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{idx} ON vacancies({idx})")

    # Таблица для хранения динамики по профессиям
    cur.execute("""CREATE TABLE IF NOT EXISTS profession_dynamics (
        profession TEXT PRIMARY KEY,
        demand_index REAL,
        avg_ttl_days REAL,
        calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    conn.commit()
    return conn, cur


# ==================================================
# HH.RU ЗАПРОСЫ
# ==================================================
def fetch_vacancies(profession: str, page: int) -> Optional[Dict]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }
    if hh_token:
        headers['Authorization'] = f'Bearer {hh_token}'

    try:
        # Для ежедневного сбора используем широкий диапазон "за последние 30 дней",
        # чтобы собирать свежие и не пропускать возобновлённые.
        date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        date_to = datetime.now().strftime('%Y-%m-%d')

        resp = requests.get("https://api.hh.ru/vacancies", headers=headers, timeout=15, params={
            "text": profession,
            "area": AREA,
            "per_page": PER_PAGE,
            "page": page,
            "search_field": "name",
            "date_from": date_from,
            "date_to": date_to,
            "order_by": "publication_time"
        })
        return resp.json() if resp.status_code == 200 else None
    except:
        return None


def fetch_details(vacancy_id: str) -> Optional[Dict]:
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    if hh_token:
        headers['Authorization'] = f'Bearer {hh_token}'
    try:
        resp = requests.get(f"https://api.hh.ru/vacancies/{vacancy_id}", headers=headers, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except:
        return None


# ==================================================
# ОБРАБОТКА ВАКАНСИЙ
# ==================================================
def parse_vacancy(vacancy: Dict, profession: str) -> Optional[Dict]:
    try:
        details = fetch_details(vacancy.get('id'))
        if not details:
            return None

        skills_list = details.get('key_skills', [])
        if not skills_list:
            return None

        salary = vacancy.get('salary', {}) or {}
        salary_from = salary.get('from')
        salary_to = salary.get('to')
        if not salary_from and not salary_to:
            return None

        skills = ", ".join([s.get('name', '') for s in skills_list])

        return {
            'profession': profession,
            'title': vacancy.get('name', '')[:100],
            'company': vacancy.get('employer', {}).get('name', '')[:100],
            'city': vacancy.get('area', {}).get('name', '')[:50],
            'salary_from': salary_from,
            'salary_to': salary_to,
            'salary_currency': salary.get('currency'),
            'key_skills': skills,
            'url': vacancy.get('alternate_url', ''),
            'published_at': vacancy.get('published_at', '')
        }
    except:
        return None


def save_or_update_vacancy(cur: sqlite3.Cursor, data: Dict) -> str:
    """
    Возвращает 'new' если вставлена, 'updated' если обновлена last_seen, 'error' при ошибке.
    """
    try:
        # Проверяем, есть ли уже URL
        cur.execute("SELECT id, is_active FROM vacancies WHERE url = ?", (data['url'],))
        row = cur.fetchone()
        if row:
            # Обновляем last_seen и возвращаем в актив
            cur.execute("UPDATE vacancies SET last_seen = CURRENT_TIMESTAMP, is_active = 1 WHERE id = ?", (row[0],))
            return 'updated'
        else:
            # Новая запись
            cur.execute("""INSERT INTO vacancies (
                profession, title, company, city,
                salary_from, salary_to, salary_currency,
                key_skills, url, published_at, last_seen, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 1)""", (
                data['profession'], data['title'], data['company'],
                data['city'], data['salary_from'], data['salary_to'],
                data['salary_currency'], data['key_skills'],
                data['url'], data['published_at']
            ))
            return 'new'
    except sqlite3.IntegrityError:
        return 'error'


def mark_inactive_vacancies(cur: sqlite3.Cursor):
    """Помечаем как неактивные все вакансии, которые не были обновлены сегодня."""
    cur.execute("UPDATE vacancies SET is_active = 0 WHERE date(last_seen) < date('now')")


def save_logs(status: int, message):
    DateAndTime = datetime.now().strftime("%d.%m.%Y %H:%M")
    with open('logs.txt', 'a', encoding='utf-8') as file:
        if status == 1:
            file.write(f"✅ | {DateAndTime} | Проверено: {message[0]} | Сохранено: {message[1]}\n")
        elif status == 0:
            file.write(f"🚫 | {DateAndTime} | {message}\n")
        else:
            file.write(f"❌ | {DateAndTime} | {message}\n")


# ==================================================
# ДИНАМИКА РЫНКА
# ==================================================
def compute_demand_index(cur: sqlite3.Cursor, profession: str) -> float:
    """
    (кол-во активных вакансий за последние 14 дней - кол-во за предыдущие 14 дней) / кол-во за предыдущие 14 дней.
    Возвращает 0.0 если недостаточно данных.
    """
    today = datetime.now().date()
    period1_start = today - timedelta(days=14)
    period1_end = today
    period2_start = today - timedelta(days=28)
    period2_end = today - timedelta(days=14)

    cur.execute("""
        SELECT COUNT(*) FROM vacancies
        WHERE profession = ? 
          AND is_active = 1
          AND date(published_at) BETWEEN ? AND ?
    """, (profession, period1_start.isoformat(), period1_end.isoformat()))
    count_recent = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM vacancies
        WHERE profession = ?
          AND is_active = 1
          AND date(published_at) BETWEEN ? AND ?
    """, (profession, period2_start.isoformat(), period2_end.isoformat()))
    count_prev = cur.fetchone()[0]

    if count_prev == 0:
        return 0.0
    return round((count_recent - count_prev) / count_prev, 4)


def compute_avg_ttl(cur: sqlite3.Cursor, profession: str) -> Optional[float]:
    """
    Среднее время жизни закрытых вакансий (в днях).
    TTL = last_seen - published_at (в днях).
    """
    cur.execute("""
        SELECT AVG(julianday(last_seen) - julianday(published_at))
        FROM vacancies
        WHERE profession = ?
          AND is_active = 0
          AND last_seen IS NOT NULL
          AND published_at IS NOT NULL
    """, (profession,))
    row = cur.fetchone()
    if row and row[0]:
        return round(row[0], 1)
    return None


def update_profession_dynamics(cur: sqlite3.Cursor):
    """Пересчитывает и сохраняет динамику для всех профессий."""
    for prof in PROFESSIONS:
        demand = compute_demand_index(cur, prof)
        ttl = compute_avg_ttl(cur, prof)
        cur.execute("""INSERT OR REPLACE INTO profession_dynamics 
                       (profession, demand_index, avg_ttl_days, calculated_at)
                       VALUES (?, ?, ?, CURRENT_TIMESTAMP)""",
                    (prof, demand, ttl))


# ==================================================
# ГЛАВНЫЙ ЦИКЛ
# ==================================================
def main():
    print("=" * 60)
    print("🚀 ПАРСЕР ВАКАНСИЙ HH.RU + РЫНОЧНАЯ ДИНАМИКА")
    print("=" * 60)

    if not get_token():
        print("❌ Не удалось получить токен")
        save_logs(-1, "Не удалось получить токен")
        return

    print("🔑 Авторизация OK")

    conn, cur = init_db()
    total_new = 0
    total_updated = 0
    total_checked = 0
    start_time = datetime.now()

    # 1. Помечаем неактивными старые вакансии (не обновлённые сегодня)
    mark_inactive_vacancies(cur)
    conn.commit()

    for prof_idx, profession in enumerate(PROFESSIONS):
        print(f"\n📌 [{prof_idx + 1}/{len(PROFESSIONS)}] {profession}")
        prof_new = 0
        prof_updated = 0

        for page in range(MAX_PAGES):
            data = fetch_vacancies(profession, page)
            if not data:
                save_logs(0, f"Ошибка запроса для [{profession}]")
                print("  ❌ Ошибка запроса")
                break

            items = data.get('items', [])
            if not items:
                save_logs(0, f"Нет вакансий для [{profession}]")
                print("  🚫 нет вакансий")
                break

            print(f"  └─ Страница {page + 1}: {len(items)} вакансий (всего: {data.get('found', 0)})")

            for vac in items:
                total_checked += 1
                parsed = parse_vacancy(vac, profession)
                if parsed is None:
                    continue

                status = save_or_update_vacancy(cur, parsed)
                if status == 'new':
                    total_new += 1
                    prof_new += 1
                elif status == 'updated':
                    total_updated += 1
                    prof_updated += 1

                time.sleep(random.uniform(*PAUSE_VACANCY))

            conn.commit()
            print(f"      ✅ Новых: {prof_new}, обновлено: {prof_updated}")
            time.sleep(random.uniform(*PAUSE_PAGE))

            if len(items) < PER_PAGE:
                print("  📄 Конец списка")
                break

        print(f"  📊 Всего новых: {prof_new}, обновлено: {prof_updated}")
        if prof_idx < len(PROFESSIONS) - 1:
            wait = random.uniform(*PAUSE_PROFESSION)
            print(f"  ⏳ Пауза {wait:.1f} сек...")
            time.sleep(wait)

    # 2. Пересчитываем рыночную динамику по всем профессиям
    update_profession_dynamics(cur)
    conn.commit()

    # 3. Выводим сводку по динамике
    print("\n" + "=" * 60)
    print("📈 РЫНОЧНАЯ ДИНАМИКА ПО ПРОФЕССИЯМ")
    print("=" * 60)
    cur.execute("SELECT * FROM profession_dynamics ORDER BY profession")
    for row in cur.fetchall():
        prof, demand_idx, ttl, calc_at = row
        ttl_str = f"{ttl} дн." if ttl else "нет данных"
        print(f"  {prof}: demand_index = {demand_idx}, средний TTL = {ttl_str} (расчёт: {calc_at})")

    elapsed = datetime.now() - start_time
    print("\n" + "=" * 60)
    print(f"✅ ГОТОВО | Новых: {total_new} | Обновлено: {total_updated} | Проверено: {total_checked}")
    print(f"⏱️  Время: {elapsed.total_seconds():.1f} сек")
    print("=" * 60)

    save_logs(1, [total_checked, total_new])

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
import os
import csv
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

BASE_URL = "https://parser-api.com/parser/fedresurs_api"

CACHE_DIR = Path("cache")
OUTPUT_DIR = Path("output")

PERSONS_CACHE_FILE = CACHE_DIR / "persons.json"
MESSAGES_CACHE_FILE = CACHE_DIR / "messages.json"


def load_cache(path):
    """Загрузить информацию об уже проведенном поиске персоны"""
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_cache(path, data):
    """Сохранить информацию о проведенном поиске"""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def api_get(endpoint, params):
    """GET запрос на эндпоинт"""

    print(f"Делаем запрос к сервису {endpoint}")

    url = f"{BASE_URL}/{endpoint}"

    while True:
        r = requests.get(url, params=params)

        if r.status_code == 200:
            data = r.json()

            if data.get("success") == 1:
                return data

            if "error" in data:
                raise Exception(f"status code: {r.status_code}, {data["error"]}")

        time.sleep(1)


def read_input():
    """Читаем файл с фамилиями """
    people = []

    with open("input.txt", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]

    for i in range(0, len(lines), 2):
        fio = lines[i]
        dob = lines[i + 1]

        last, first, patronymic = fio.split()

        people.append({
            "last": last,
            "first": first,
            "patronymic": patronymic,
            "dob": dob
        })

    return people


def ensure_dirs():
    """Создаем папочки если их нет"""
    CACHE_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)


def append_csv(path, row, header):
    file_exists = path.exists()

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)


def search_person(api_key, person):
    """Поиск персоны"""
    params = {
        "key": api_key,
        "lastName": person["last"],
        "firstName": person["first"],
        "patronymic": person["patronymic"]
    }

    data = api_get("search_fiz", params)

    return data.get("records", [])


def get_person(api_key, person_id):
    """Получить персону"""
    params = {
        "key": api_key,
        "id": person_id
    }

    data = api_get("get_person", params)

    return data["record"]


def get_messages(api_key, person_id):
    """Получить несколько сообщений о персоне"""

    start = 0

    while True:
        params = {
            "key": api_key,
            "id": person_id,
            "from_record": start
        }

        data = api_get("get_person_messages", params)

        records = data.get("records", [])

        if not records:
            break

        for r in records:
            yield r

        start += len(records)


def get_message(api_key, msg_id):
    """Получить 1 сообщение о персоне"""
    params = {
        "key": api_key,
        "id": msg_id
    }

    data = api_get("get_message", params)

    return data["record"]


def process_person(api_key, person, persons_cache, messages_cache):
    """Поиск информации о персоне в сервисе"""

    key = f'{person["last"]}|{person["first"]}|{person["patronymic"]}|{person["dob"]}'

    if key in persons_cache:
        person_id = persons_cache[key]["person_id"]
        print("cached person:", key)

    else:
        results = search_person(api_key, person)

        person_id = None

        for r in results:
            rec = get_person(api_key, r["id"])

            if rec["dob"] == person["dob"]:
                person_id = r["id"]

                append_csv(
                    OUTPUT_DIR / "persons.csv",
                    rec,
                    rec.keys()
                )

                break

        persons_cache[key] = {"person_id": person_id}

    if not person_id:
        print("not found:", key)
        return

    for msg in get_messages(api_key, person_id):

        msg_id = msg["id"]

        if msg_id in messages_cache:
            continue

        details = get_message(api_key, msg_id)

        append_csv(
            OUTPUT_DIR / "messages.csv",
            details,
            details.keys()
        )

        if "lots" in details:
            for lot in details["lots"]:

                lot["message_id"] = msg_id

                append_csv(
                    OUTPUT_DIR / "lots.csv",
                    lot,
                    lot.keys()
                )

        messages_cache[msg_id] = True


def main():

    load_dotenv()

    api_key = os.getenv("API_KEY")

    if not api_key:
        raise Exception("API_KEY missing")

    ensure_dirs()

    persons_cache = load_cache(PERSONS_CACHE_FILE)
    messages_cache = load_cache(MESSAGES_CACHE_FILE)

    people = read_input()

    print("people:", people)

    for person in people:

        try:
            process_person(
                api_key,
                person,
                persons_cache,
                messages_cache
            )

        except Exception as e:
            print("error:", e)

    save_cache(PERSONS_CACHE_FILE, persons_cache)
    save_cache(MESSAGES_CACHE_FILE, messages_cache)

    print("done")


if __name__ == "__main__":
    main()

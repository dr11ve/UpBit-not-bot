import os, time, json, re, hashlib, logging, threading
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup
from telegram import Bot
from fastapi import FastAPI
import uvicorn

from dotenv import load_dotenv

# ===================== ЗАГРУЗКА tg.txt =====================
ROOT_DIR = Path(__file__).resolve().parent
env_file = ROOT_DIR / "tg.txt"

if load_dotenv(env_file):
    print(f"✓ Загружен файл окружения: {env_file}")
else:
    print(f"⚠️ Файл {env_file} не найден. Переменные окружения не загружены.")

# ===================== НАСТРОЙКИ + ПРОВЕРКА ТОКЕНА =====================
TELEGRAM_TOKEN_RAW = os.getenv("TG_TOKEN", "")
CHAT_ID = os.getenv("TG_CHAT_ID")
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "60"))
FILTER_MARKETS = [p.strip().upper() + "-" for p in os.getenv("FILTER_MARKETS", "KRW,USDT").split(",") if p.strip()]

# Нормализуем токен (срезаем кавычки/пробелы на концах)
TELEGRAM_TOKEN = TELEGRAM_TOKEN_RAW.strip().strip('"').strip("'")
TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{20,}$")

if not TELEGRAM_TOKEN:
    raise SystemExit("❌ TG_TOKEN пуст (см. tg.txt)")
if not TOKEN_RE.match(TELEGRAM_TOKEN):
    def cp(s):
        return [] if not s else [ord(s[0]), ord(s[-1])]
    raise SystemExit(
        f"❌ Неверный формат TG_TOKEN. Длина={len(TELEGRAM_TOKEN)}; "
        f"крайние символы (codepoints)={cp(TELEGRAM_TOKEN)}. "
        "Проверь tg.txt: TG_TOKEN=<числа:ключ> без кавычек и пробелов."
    )

if not CHAT_ID:
    print("⚠️ TG_CHAT_ID пуст — бот запустится, но не сможет слать уведомления.")

UPBIT_MARKETS_URL = "https://api.upbit.com/v1/market/all"
UPBIT_NOTICES_URL = "https://upbit.com/service_center/notice"

CACHE_FILE = str(ROOT_DIR / "upbit_markets_cache.json")
NOTICES_CACHE_FILE = str(ROOT_DIR / "upbit_notices_cache.json")

HDRS = {"User-Agent": "UpbitListingsBot/1.3 (+local)"}

# ===================== HTTP с ретраями =====================
def build_http():
    s = requests.Session()
    try:
        retries = Retry(
            total=3, connect=3, read=3, status=3,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
        )
    except TypeError:
        # совместимость со старыми версиями urllib3
        retries = Retry(
            total=3, connect=3, read=3, status=3,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            method_whitelist=frozenset(["GET", "HEAD"]),
        )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HDRS)
    return s

HTTP = build_http()

# ===================== ЛОГИ =====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("upbit-bot")

# ===================== TELEGRAM КЛИЕНТ =====================
bot = Bot(token=TELEGRAM_TOKEN)

# ===================== HEALTH =====================
last_cycle_ok = True
last_cycle_at = None

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": last_cycle_ok, "last_cycle_at": last_cycle_at, "filter_markets": FILTER_MARKETS}

def run_http():
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="warning")

def mark_ok():
    global last_cycle_ok, last_cycle_at
    last_cycle_ok = True
    last_cycle_at = datetime.now(timezone.utc).isoformat()

def mark_fail():
    global last_cycle_ok
    last_cycle_ok = False

# ===================== УТИЛИТЫ =====================
def load_cache(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_cache(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def send_message(text: str):
    if not CHAT_ID:
        log.warning("CHAT_ID пуст, пропускаю отправку: %s", text)
        return
    try:
        bot.send_message(chat_id=CHAT_ID, text=text)
    except Exception as e:
        log.error("Ошибка отправки в Telegram (chat_id=%s): %s", CHAT_ID, e)

# ===================== ИСТОЧНИКИ ДАННЫХ =====================
def fetch_markets():
    r = HTTP.get(UPBIT_MARKETS_URL, timeout=30)
    r.raise_for_status()
    return r.json()  # [{market, korean_name, english_name, ...}]

NOTICE_MARKET_PATTERN = re.compile(r"(KRW|원화|USDT|테더|유에스디티)", re.I)

def fetch_listing_notices():
    try:
        resp = HTTP.get(UPBIT_NOTICES_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except requests.Timeout:
        log.warning("Notice timeout: %s", UPBIT_NOTICES_URL)
        return []
    except requests.RequestException as e:
        log.warning("Notice request error: %s", e)
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.find_all("a", href=True):
        title = (a.get_text() or "").strip()
        href = a["href"]
        if not title or not href:
            continue
        if href.startswith("/"):
            href = "https://upbit.com" + href
        # ключевые слова листинга
        if re.search(r"(상장|리스트|listing|마켓\s*추가|market\s*support|new\s*listing)", title, re.I):
            # фильтр по KRW/USDT в заголовке Notice
            if not NOTICE_MARKET_PATTERN.search(title):
                continue
            uniq = hashlib.sha256((title + "|" + href).encode("utf-8")).hexdigest()[:16]
            items.append({"id": uniq, "title": title, "url": href})
    return items

# ===================== ЛОГИКА =====================
def detect_new_markets(old_set, markets):
    current = {m["market"] for m in markets}
    return sorted(current - old_set), current

def _passes_prefix_filter(market_code: str) -> bool:
    up = market_code.upper()
    return any(up.startswith(pref) for pref in FILTER_MARKETS)

def notify_new_markets(new_markets, markets):
    filtered = [mk for mk in new_markets if _passes_prefix_filter(mk)]
    if not filtered:
        return
    info = {m["market"]: m for m in markets}
    lines = []
    for mk in filtered:
        eng = info[mk].get("english_name", "")
        kor = info[mk].get("korean_name", "")
        lines.append(f"• {mk} — {eng} / {kor}")
    text = (
        "🆕 Upbit: новые рынки (" + ",".join(p.rstrip('-') for p in FILTER_MARKETS) + ")\n"
        + "\n".join(lines)
        + f"\n\nИсточник API: {UPBIT_MARKETS_URL}"
    )
    send_message(text)

def notify_new_notices(new_items):
    if not new_items:
        return
    lines = [f"• {it['title']}\n  {it['url']}" for it in new_items]
    send_message("📢 Upbit: новое объявление о листинге (KRW/USDT):\n" + "\n\n".join(lines))

def bootstrap_baseline():
    """Первый запуск: фиксируем текущее состояние, чтобы не спамить историей."""
    try:
        markets = fetch_markets()
        current_markets = sorted({m["market"] for m in markets})
        save_cache(CACHE_FILE, {"markets": current_markets})
        log.info("Baseline markets saved: %d", len(current_markets))
    except Exception as e:
        log.exception("Bootstrap markets failed: %s", e)

    try:
        notices = fetch_listing_notices()
        ids = sorted({n["id"] for n in notices})
        save_cache(NOTICES_CACHE_FILE, {"ids": ids})
        log.info("Baseline notices saved: %d", len(ids))
    except Exception as e:
        log.exception("Bootstrap notices failed: %s", e)

def main_loop():
    global last_cycle_ok
    markets_cache = load_cache(CACHE_FILE, {"markets": []})
    known_markets = set(markets_cache.get("markets", []))

    notices_cache = load_cache(NOTICES_CACHE_FILE, {"ids": []})
    known_notice_ids = set(notices_cache.get("ids", []))

    send_message("🚀 Upbit бот запущен")

    while True:
        # 1) рынки (критично)
        try:
            markets = fetch_markets()
            new_markets, current = detect_new_markets(known_markets, markets)
            if new_markets:
                log.info("Найдены новые рынки (до фильтра): %s", new_markets)
                notify_new_markets(new_markets, markets)
                known_markets = current
                save_cache(CACHE_FILE, {"markets": sorted(list(known_markets))})
            markets_ok = True
        except Exception as e:
            markets_ok = False
            log.exception("Ошибка markets: %s", e)

        # 2) объявления (некритично)
        try:
            notices = fetch_listing_notices()
            fresh = [n for n in notices if n["id"] not in known_notice_ids]
            if fresh:
                log.info("Найдены новые листинговые объявления: %d", len(fresh))
                notify_new_notices(fresh)
                known_notice_ids |= {n["id"] for n in fresh}
                save_cache(NOTICES_CACHE_FILE, {"ids": sorted(list(known_notice_ids))})
            notices_ok = True
        except Exception as e:
            notices_ok = False
            log.exception("Ошибка notices: %s", e)

        if markets_ok:
            mark_ok()
        else:
            mark_fail()

        time.sleep(POLL_INTERVAL_SEC)

# ===================== ЗАПУСК =====================
if __name__ == "__main__":
    threading.Thread(target=run_http, daemon=True).start()

    if not os.path.exists(CACHE_FILE) or not os.path.exists(NOTICES_CACHE_FILE):
        log.info("Cache not found, creating baseline...")
        bootstrap_baseline()

    log.info("Фильтр рынков: %s", ",".join(p.rstrip('-') for p in FILTER_MARKETS))
    main_loop()
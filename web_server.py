"""Web 管理介面模組。

提供 FastAPI Web 伺服器，整合每日排程上傳與手動上傳功能。
支援透過網頁操作手動上傳指定日期的資料，以及修改每日排程時間。
"""

import os
import re
import itertools
import json
import uuid
import time
import random
import threading
import logging
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager

import schedule as schedule_lib
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from easydict import EasyDict
from sqlalchemy import text

from dataclasses import asdict

from DailyUpload import daily_craw, set_retry_queue, DB_NAMES, HOST, USER, PASSWORD, CRAWLERHOST
from upload import day_upload
from data_upload.base import (
    CrawlError,
    NetworkError,
    OutOfRangeError,
    SourceError,
)
from data_upload.quarter_revenue import QuarterRevenueUploader
from data_upload.tdcc import TDCCUploader
from data_upload.ctee_news import CTEENewsUploader
from data_upload.cnyes_news import CNYESNewsUploader
from data_upload.ptt_news import PTTNewsUploader
from data_upload.moneyudn_news import MoneyUDNNewsUploader
from data_upload.company_info import CompanyInfoUploader
from data_upload.yt_transcript import YTTranscriptUploader
from data_upload.oil_price import OilPriceUploader
from data_upload.gold_price import GoldPriceUploader
from data_upload.bitcoin_price import BitcoinPriceUploader
from data_upload.currency_price import CurrencyPriceUploader
from data_upload.indices_price import IndicesPriceUploader
from data_upload import special_info_common
from retry_queue import RetryQueue, is_network_error, check_network_available
from routers import db_conn

# 路徑設定
BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
LOG_DIR = BASE_DIR / "logs"

# 持久化設定目錄：**刻意與 logs/ 分離**。設定檔曾寄生在 log 目錄（logs/config.json），
# 但部署（CI deploy）把 logs 掛成具名 volume、手動 run.sh 掛 host 的 ./logs，兩條路徑
# 的 log 目錄內容不同，設定會隨掛載方式在容器間「靜默消失」（Web 介面改完排程，換個
# 啟動方式就回到程式碼預設值）。故設定改放獨立的 config/ 目錄，run.sh 與 CI deploy
# 掛載**同一個** host 絕對路徑，兩條路徑不再分岔。
CONFIG_DIR = Path(os.environ.get("CONFIG_DIR") or (BASE_DIR / "config"))
CONFIG_PATH = CONFIG_DIR / "config.json"

# 舊設定位置（寄生在 log 目錄），僅供一次性遷移讀取，不再寫入。
LEGACY_CONFIG_PATH = LOG_DIR / "config.json"
# 一次性遷移成功後，舊設定檔改名保留為備份（避免每次啟動重複判讀新舊來源）。
LEGACY_CONFIG_BACKUP_PATH = LOG_DIR / "config.json.migrated"

# 確保 logs 與 config 資料夾存在
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)

# Logging 設定
log_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler = logging.FileHandler(LOG_DIR / "web_server.log")
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

# 設定 root logger 讓所有子模組的 log 都能輸出
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# 上傳任務追蹤
upload_jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()

# 排程管理
schedule_lock = threading.Lock()

# daily_craw 重入控制。
#
# daily_craw 會逐一對爬蟲請求「近 30 天內未上傳的日期」，耗時隨缺漏天數增長；
# 若直接在 scheduler_thread 內同步執行，會在 run_pending() 期間持有 schedule_lock，
# 使當日後續排程（TDCC/商品價格/新聞/YT/自我修復）全部無法準時觸發。
# 2026-08 事故中曾因此延後逾 20 小時，導致新聞回溯窗（僅約 3 天）錯過而永久缺資料。
# 故改為背景執行緒執行，並以此旗標確保同時只有一輪 daily_craw 在跑。
daily_craw_lock = threading.Lock()
daily_craw_running = False

# 重試佇列重入控制。
#
# process_retry_queue 會**同步**重跑佇列內每一筆任務，新聞類任務是整個 48 小時窗
# 重抓（CTEE 實測 210~230 秒），一輪累積下來可達數分鐘至數十分鐘。與 daily_craw
# 同理，直接由 scheduler_thread 呼叫會在 run_pending() 期間持有 schedule_lock，
# 把當日 21:00~21:30 的爬取排程整批往後推。爬蟲 v2.14.0 起 PTT／MoneyUDN 的零星
# 抓漏由 ok 改回報 partial，排入重試的頻率大幅提高，此風險已從理論變成常態。
# 故一律改以背景執行緒執行，並以此旗標確保同時只有一輪重試在跑
# （手動觸發的 /api/retry-queue/retry-all 亦共用，避免與排程輪重複執行同一任務）。
retry_queue_lock = threading.Lock()
retry_queue_running = False

# 網路失敗重試佇列
retry_queue: RetryQueue | None = None

# 隔日重排時間：每日將未達上限的 exhausted 任務重設為 pending 再試一輪。
# 屬維護型排程，不受爬蟲集中時間窗政策約束，故 v3 搬窗時維持 06:30 不變：
# 仍落在「當日各爬取排程（21:00~21:27）之前」，前一晚因「資料尚未發布」而放棄的
# 任務會在隔天一早重設為 pending，再由每小時的重試佇列於白天陸續補回。
REQUEUE_EXHAUSTED_TIME = "06:30"

# SPECIAL_INFO 缺漏自我修復掃描窗（天數）：遠大於各商品排程的「過去 7 天」
# 回補窗，足以自癒管線停擺數週造成的缺漏。
SPECIAL_INFO_BACKFILL_DAYS = 30

# SPECIAL_INFO 排程補抓時要一併重驗的孤兒帳本天數。
#
# 「帳本有列、價格表 0 列」的孤兒有兩種：合法的非交易日標記，以及誤標。
# 誤標一旦寫入，find_missing_dates 就永遠不會再把該日列為候選，只能靠人工
# deep 重驗才救得回來——2026-08 的孤兒就是這樣累積的。故日常排程也清一小段
# 近期窗並重驗：窗口取小（不是整個 30 天）是為了不要每天重問幾十個早已確認
# 的非交易日，7 天足以在誤標當週自癒。
SPECIAL_INFO_REVERIFY_DAYS = 7

# 新聞排程回溯時數：四來源新聞排程於 21:16~21:22 抓取，回溯窗須涵蓋「昨日整天到
# 現在」。設為 48 小時（爬蟲支援 1~72），可補回偶發漏抓一日；重複抓取的記錄由各
# 上傳器以 URL 去重（同日已存在的 URL 會被跳過），不會重複寫入資料庫或全文檔。
# **搬窗後涵蓋範圍只增不減**：48 小時是「往回推」的相對窗，21:16 執行時涵蓋前日
# 21:16 至今，昨日整天必定完整包含在內（早上 07:46 執行時亦然），故不需調整時數。
NEWS_SCHEDULE_HOURS = 48

# SPECIAL_INFO 五個商品的 (task_type, Uploader 類別) 對照，供缺漏自我修復
# 作業逐一掃描補抓；task_type 與 retry_queue／各 upload 端點一致。
SPECIAL_INFO_ASSETS = [
    ("oil_price", OilPriceUploader),
    ("gold_price", GoldPriceUploader),
    ("bitcoin_price", BitcoinPriceUploader),
    ("currency_price", CurrencyPriceUploader),
    ("indices_price", IndicesPriceUploader),
]

# 任務佇列
from job_queue import JobQueue
job_queue: JobQueue | None = None


def _validate_date_format(date_str):
    """驗證日期格式是否為 YYYY-MM-DD。

    Args:
        date_str: 日期字串。

    Returns:
        bool: 格式正確回傳 True。
    """
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_str))


# 設定檔結構版本：每次需要對「已持久化」的 config.json 做一次性批次調整時遞增。
# v2：把所有爬蟲抓取排程集中至單一時間窗（見 _migrate_crawl_schedule_window）。
# v3：時間窗自 07:30~08:00 整體平移至 21:00~21:30。宿主 Mac 長期處於睡眠循環，
#     Docker VM 隨之凍結；以 Prometheus 逐分鐘樣本統計 7 天，容器「實際醒著」
#     的比率 07:00 僅 36.9%、08:00 為 50.5%（且逐日 4%~100% 極不穩定），而
#     21:00 為 93.8%（全日最高）、20:00/22:00 亦逾 91%。相對間隔完全不變，
#     整批往後移 13.5 小時。
CONFIG_VERSION = 3

# 爬蟲抓取排程統一集中的目標時間窗（HH:MM，含端點）。
CRAWL_WINDOW_START = "21:00"
CRAWL_WINDOW_END = "21:30"

# 屬於「爬蟲抓取類」的具名排程鍵（其時間受 21:00~21:30 集中政策約束）；
# daily_craw 主排程為頂層 schedule_time，另行處理。retry queue／隔日重排等
# 維護型排程不在此列、不受遷移影響。
CRAWL_SCHEDULE_KEYS = (
    "tdcc_schedule",
    "ctee_schedule",
    "cnyes_schedule",
    "ptt_schedule",
    "moneyudn_schedule",
    "yt_transcript_schedule",
    "oil_price_schedule",
    "gold_price_schedule",
    "bitcoin_price_schedule",
    "currency_price_schedule",
    "indices_price_schedule",
    "special_info_backfill_schedule",
)

# 舊版曾用、但恰好落在新窗邊界內、不會被 _in_crawl_window 判為窗外的預設值。
# 一次性遷移時需一併收斂到新預設，否則既有部署會停在舊值。v3 的兩個案例都來自
# v1 的晚間預設：CTEE 舊預設 21:00（== 新窗起點，且與 daily_craw 撞在同一分鐘）、
# CNYES 舊預設 21:30（== 新窗終點）。v1 其餘晚間預設（PTT 22:00、MoneyUDN 22:30、
# YT 19:05、TDCC 10:00）與 v2 的 07:xx 全部落在新窗外，由 _in_crawl_window 處理。
# **日後再搬窗時務必重新盤點**：漏列的鍵會靜默停在舊值。
_SUPERSEDED_IN_WINDOW_DEFAULTS = {
    "ctee_schedule": "21:00",
    "cnyes_schedule": "21:30",
}


# 排程時間字串的合法格式（HH:MM，24 小時制）。schedule 套件的 .at() 只吃這種格式，
# 其餘值（None、數字、"sunday 21:03"）會讓排程註冊當場拋例外而使服務起不來。
# **務必以 fullmatch 使用**：本 pattern 不含 ^$ 錨點，改用 match／search 會讓
# "21:00:00"、"21:00\n" 被靜默放行。
_TIME_PATTERN = re.compile(r"([01]\d|2[0-3]):[0-5]\d")


def _is_valid_time(value):
    """判斷值是否為合法的 HH:MM 時間字串。

    Args:
        value: 待檢查的值（任意型別）。

    Returns:
        bool: 為合法 HH:MM 字串時回傳 True。
    """
    return isinstance(value, str) and bool(_TIME_PATTERN.fullmatch(value))


def _in_crawl_window(time_str):
    """判斷 HH:MM 是否落在爬蟲集中時間窗 21:00~21:30（含端點）內。

    因 HH:MM 為零填補的等長字串，字典序比較等同時間先後比較。

    Args:
        time_str (str | None): 時間字串（HH:MM）。

    Returns:
        bool: 落在窗內回傳 True；None 或非字串回傳 False。
    """
    if not isinstance(time_str, str):
        return False
    return CRAWL_WINDOW_START <= time_str <= CRAWL_WINDOW_END


def _migrate_crawl_schedule_window(config, default):
    """一次性遷移：把仍落在舊時段的爬蟲抓取排程收斂到 21:00~21:30 新預設。

    僅在既有設定的 config_version 低於 CONFIG_VERSION 時由 load_config 觸發一次
    （version-gated），避免日後每次重啟都覆蓋使用者經 Web 介面所做的自訂。遷移時
    只重置「時間落在目標窗外」的鍵，已在窗內者保留，尊重既有的窗內微調。

    Args:
        config (dict): 既有設定內容（就地修改）。
        default (dict): 內含新版預設時間的預設設定。

    Returns:
        bool: 是否有任何欄位被修改。
    """
    changed = False

    # daily_craw 主排程（頂層字串）。注意：這裡刻意沒有 superseded-default
    # 防線，因為 v1/v2 的歷史值（20:07、07:30）都落在新窗外；日後若某次搬窗
    # 讓歷史值落進新窗內，此處也要比照 _SUPERSEDED_IN_WINDOW_DEFAULTS 處理。
    if not _in_crawl_window(config.get("schedule_time")):
        if config.get("schedule_time") != default["schedule_time"]:
            config["schedule_time"] = default["schedule_time"]
            changed = True

    # 各具名爬蟲排程（{"time": HH:MM} 結構）
    for key in CRAWL_SCHEDULE_KEYS:
        entry = config.get(key)
        cur = entry.get("time") if isinstance(entry, dict) else None
        # 窗外，或恰為「落在窗內的舊版預設」→ 收斂到新預設。
        if (not _in_crawl_window(cur)
                or cur == _SUPERSEDED_IN_WINDOW_DEFAULTS.get(key)):
            new_entry = dict(default[key])
            if config.get(key) != new_entry:
                config[key] = new_entry
                changed = True

    return changed


# 「新舊設定並存」的 warning 只在第一次記錄：load_config 幾乎每個 API 端點都會呼叫，
# 而舊檔依設計不會被自動刪除，若每次都記 warning 會把 log 洗掉。
_legacy_coexist_warned = False
# 舊設定檔讀取失敗的 warning 同理只記第一次。
_legacy_read_warned = False
# 上一次警告過的「格式不符欄位」組合（tuple），用來避免同一組問題重複刷 warning。
_repaired_fields_warned = ()


# 設定檔暫存檔名的遞增序號（同一行程內唯一，配合 pid 即可避免併發互相覆蓋）。
_config_tmp_counter = itertools.count()


def read_config_file(path):
    """讀取設定檔並驗證其形狀。

    刻意把「讀不到」與「內容壞掉」分成兩種例外，因為兩者的正確處置相反：前者不該
    動使用者的檔案，後者才需要隔離。

    Args:
        path (pathlib.Path): 設定檔路徑。

    Returns:
        dict: 設定內容。

    Raises:
        OSError: 檔案讀取失敗（權限不足、暫時性 IO 錯誤等），**不代表內容毀損**。
        ValueError: 內容無法解析或頂層不是 JSON 物件。涵蓋 json.JSONDecodeError 與
            UnicodeDecodeError（兩者皆為 ValueError 子類），以及 list/int 等非物件
            內容——後者若直接回傳，會在後續補欄位時炸成 TypeError。
    """
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)
    if not isinstance(config, dict):
        raise ValueError(
            f"設定檔頂層須為 JSON 物件，實際為 {type(config).__name__}"
        )
    return config


def quarantine_corrupt_config():
    """把毀損的新設定檔改名隔離，讓後續流程能退回預設值或重新搬遷舊設定。

    設定檔毀損（例如寫入途中被中斷）時若原地留著，migrate_legacy_config 會因為
    「新位置已存在」而永不搬遷舊設定，且每次 load_config 都會拋例外導致服務起不來。
    改名為 config.json.corrupt 後保留現場供人工檢視，服務則可正常啟動。

    Returns:
        bool: 成功改名時回傳 True，否則 False。
    """
    corrupt_path = CONFIG_PATH.with_name(CONFIG_PATH.name + ".corrupt")
    try:
        CONFIG_PATH.rename(corrupt_path)
    except OSError as exc:
        logger.error("毀損設定檔 %s 改名隔離失敗：%s", CONFIG_PATH, exc)
        return False
    logger.error("設定檔 %s 無法解析，已改名為 %s 並改用預設值（或改讀舊設定）。",
                 CONFIG_PATH, corrupt_path)
    return True


def migrate_legacy_config():
    """把寄生在 log 目錄的舊設定檔一次性搬遷到獨立設定目錄。

    舊版把 config.json 放在 logs/ 內，而部署時 logs/ 是具名 volume、手動 run.sh 是
    host bind，導致設定隨掛載方式而消失。本函式在服務讀取設定前執行，若新位置尚無
    設定、舊位置有，就**原樣**（含 config_version）搬過去，讓既有排程自訂不被丟棄，
    且後續 version-gated 的一次性遷移仍照原語意判讀。

    優先順序：新位置（CONFIG_PATH）永遠優先。兩邊都存在時不覆寫新位置，只記錄
    warning 提示舊檔已失效；兩邊都不存在時不做任何事（由 load_config 回傳預設值）。
    搬遷成功後把舊檔改名為 config.json.migrated 保留備份，避免每次啟動重複判讀。

    Returns:
        bool: 實際發生搬遷時回傳 True，否則 False。
    """
    try:
        if not LEGACY_CONFIG_PATH.exists():
            return False
        if CONFIG_PATH.exists():
            global _legacy_coexist_warned
            if not _legacy_coexist_warned:
                logger.warning(
                    "設定檔新舊位置同時存在，一律以新位置 %s 為準；舊檔 %s 已失效，"
                    "可自行刪除。", CONFIG_PATH, LEGACY_CONFIG_PATH,
                )
                _legacy_coexist_warned = True
            else:
                logger.debug("設定檔新舊位置仍同時存在（以 %s 為準）。", CONFIG_PATH)
            return False
        legacy_config = read_config_file(LEGACY_CONFIG_PATH)
    except (OSError, ValueError) as exc:
        # ValueError 涵蓋 JSON 語法錯、非 UTF-8（UnicodeDecodeError）與頂層非物件；
        # 舊檔壞掉不該讓服務起不來，也不該把垃圾內容搬進新位置。舊檔一律保留不動，
        # 由使用者自行處置。warning 只記一次（load_config 幾乎每個端點都會呼叫）。
        global _legacy_read_warned
        if not _legacy_read_warned:
            logger.warning("讀取舊設定檔 %s 失敗，改用預設值：%s",
                           LEGACY_CONFIG_PATH, exc)
            _legacy_read_warned = True
        else:
            logger.debug("舊設定檔 %s 仍無法讀取（改用預設值）。",
                         LEGACY_CONFIG_PATH)
        return False

    try:
        save_config(legacy_config)
    except OSError as exc:
        # 寫不進新位置時**不可**動舊檔：留著它，下次啟動還能再試一次搬遷，
        # 使用者設定不會被丟掉；本輪則退回預設值啟動而非讓服務起不來。
        logger.error("舊設定搬遷失敗（無法寫入 %s）：%s；已保留舊檔 %s。",
                     CONFIG_PATH, exc, LEGACY_CONFIG_PATH)
        return False
    logger.info("已將舊設定檔 %s 一次性搬遷至 %s（設定內容原樣保留）。",
                LEGACY_CONFIG_PATH, CONFIG_PATH)
    try:
        LEGACY_CONFIG_PATH.rename(LEGACY_CONFIG_BACKUP_PATH)
    except OSError as exc:
        # 改名失敗不影響設定正確性（新位置已寫入且優先），僅下次啟動會再記一次
        # 「新舊並存」warning。
        logger.warning("舊設定檔 %s 改名備份失敗：%s", LEGACY_CONFIG_PATH, exc)
    return True


def _save_config_best_effort(config, purpose):
    """盡力寫回設定，寫不進去也不讓服務起不來。

    load_config 內的兩處自動寫回（TDCC 週排程遷移、時間窗一次性遷移）都發生在
    lifespan 啟動路徑上；若因唯讀檔案系統／權限問題拋 OSError，整個服務會起不來，
    而這兩次寫回只是「持久化本來就已算好的結果」，記憶體內的設定仍然可用。

    Args:
        config (dict): 要寫回的設定內容。
        purpose (str): 本次寫回的用途（僅供 log 描述）。
    """
    try:
        save_config(config)
    except OSError as exc:
        logger.error("%s 無法寫回設定檔 %s：%s；本次以記憶體內的設定繼續執行。",
                     purpose, CONFIG_PATH, exc)


def _normalize_config(config, default):
    """補齊缺漏欄位、修復型別不符的欄位，並執行 version-gated 的一次性遷移。

    設定檔可能被人工編輯壞（`"tdcc_schedule": null`、`"ctee_schedule": 5`、
    `"schedule_time": "sunday"`），這些值一路傳到 `setup_schedule` 才爆炸的話，
    在 `--restart always` 下就是重啟迴圈。故此處做**與版本無關**的形狀正規化：
    凡型別或格式不符預期者一律換成預設值並記 warning，讓服務照樣起得來，同時
    保留同一份設定中其餘正常的使用者自訂。

    形狀不符者本身**不觸發寫回**，保留使用者原檔供人工檢視；只有一次性遷移
    （TDCC 舊格式、爬蟲時間窗）才寫回，且統一在正規化全部完成後寫一次，避免
    半套結果落地——注意兩者同時發生時，修復後的內容會隨遷移一併寫回，warning
    文案會據此改口。同一組壞欄位的 warning 只記一次（其後降為 debug）。

    Args:
        config (dict): 讀入的既有設定（就地修改）。
        default (dict): 內含新版預設值的設定。

    Returns:
        dict: 正規化後的設定。
    """
    global _repaired_fields_warned
    write_back_reasons = []

    # 向後相容：舊格式的 tdcc_schedule 含 day 欄位，遷移為新格式（僅保留 time）。
    # 需先確認確實是 dict，否則 "day" in "sunday" 之類會誤判並在下一行拋
    # AttributeError（字串／list 沒有 .get）。
    tdcc = config.get("tdcc_schedule")
    if isinstance(tdcc, dict) and "day" in tdcc:
        config["tdcc_schedule"] = {"time": tdcc.get("time", "21:03")}
        write_back_reasons.append("TDCC 排程設定從週排程遷移為每日排程")
        logger.info("已將 TDCC 排程設定從週排程遷移為每日排程。")

    # 形狀正規化：缺鍵靜默補上（舊版設定沒有新欄位屬正常），型別／格式錯則
    # 換成預設值並記 warning（那是壞掉的設定，使用者需要知道）。
    repaired = []
    for key, default_value in default.items():
        if key == "config_version":
            continue
        if isinstance(default_value, dict):
            # {"time": "HH:MM"} 結構的具名排程
            entry = config.get(key)
            if key not in config:
                config[key] = dict(default_value)
            elif not isinstance(entry, dict) or not _is_valid_time(
                    entry.get("time")):
                config[key] = dict(default_value)
                repaired.append(key)
        else:
            # 目前 default 內的頂層純量僅有 schedule_time（HH:MM 字串）
            if key not in config:
                config[key] = default_value
            elif not _is_valid_time(config[key]):
                config[key] = default_value
                repaired.append(key)

    # config_version 缺漏（舊版設定本來就沒有）或無法判讀時一律視同最舊版本，
    # 讓一次性遷移有機會補跑；直接拿字串和 CONFIG_VERSION 比大小會拋 TypeError。
    version = config.get("config_version", 1)
    if isinstance(version, bool) or not isinstance(version, int):
        version = 1
        repaired.append("config_version")
    config["config_version"] = version

    # 一次性遷移（version-gated）：把落在 21:00~21:30 窗外的爬蟲排程收斂到
    # 新預設並寫回，讓部署重啟後持久化的舊時段自動更新；bump 版本後不再重跑，
    # 保留使用者日後經 Web 介面所做的窗內自訂。
    if config["config_version"] < CONFIG_VERSION:
        migrated = _migrate_crawl_schedule_window(config, default)
        config["config_version"] = CONFIG_VERSION
        write_back_reasons.append("爬蟲抓取排程一次性遷移至 21:00~21:30 時間窗")
        if migrated:
            logger.info(
                "已將舊時段的爬蟲抓取排程一次性遷移至 21:00~21:30 時間窗。"
            )

    if repaired:
        # 同一組壞欄位只警告一次：load_config 幾乎每個 API 端點都會呼叫，每次都記
        # 會把 log 洗掉；壞欄位換一組（使用者又改壞別的）時仍會再警告。
        fields = tuple(repaired)
        if fields != _repaired_fields_warned:
            logger.warning(
                "設定檔 %s 中下列欄位格式不符，本輪改用預設值（%s，請人工確認）：%s",
                CONFIG_PATH,
                "原檔將因本次一次性遷移一併被改寫" if write_back_reasons
                else "原檔保留未修改",
                "、".join(repaired),
            )
            _repaired_fields_warned = fields
        else:
            logger.debug("設定檔 %s 的欄位格式問題仍在：%s",
                         CONFIG_PATH, "、".join(repaired))
    else:
        # 壞欄位已被修好（例如經 Web 介面重新寫入），旗標歸零；日後又改壞同一組
        # 欄位時仍會再警告一次，而不是被誤判為「已警告過」。
        _repaired_fields_warned = ()

    if write_back_reasons:
        _save_config_best_effort(config, "、".join(write_back_reasons))
    return config


def load_config():
    """讀取設定檔。

    讀取前先呼叫 migrate_legacy_config 處理「舊設定檔仍在 log 目錄」的情形。
    首次讀取既有（舊版）config.json 時，會依 config_version 觸發一次性遷移，
    把落在 21:00~21:30 窗外的爬蟲抓取排程收斂到新預設並寫回，之後保留使用者的
    窗內自訂（見 _migrate_crawl_schedule_window）。

    Returns:
        dict: 設定內容，包含 schedule_time、tdcc_schedule、
            ctee_schedule、cnyes_schedule、ptt_schedule、
            moneyudn_schedule、yt_transcript_schedule、
            oil_price_schedule、gold_price_schedule、
            bitcoin_price_schedule、currency_price_schedule、
            indices_price_schedule、special_info_backfill_schedule
            與 config_version 欄位。
    """
    # 舊版設定寄生在 log 目錄，先一次性搬遷到獨立設定目錄，避免使用者自訂被丟棄。
    migrate_legacy_config()

    # 所有爬取排程集中於 21:00~21:30 之間並彼此錯開，避免同時併發搶爬蟲資源。
    default = {
        "config_version": CONFIG_VERSION,
        "schedule_time": "21:00",
        "tdcc_schedule": {"time": "21:03"},
        "ctee_schedule": {"time": "21:16"},
        "cnyes_schedule": {"time": "21:18"},
        "ptt_schedule": {"time": "21:20"},
        "moneyudn_schedule": {"time": "21:22"},
        "yt_transcript_schedule": {"time": "21:24"},
        "oil_price_schedule": {"time": "21:06"},
        "gold_price_schedule": {"time": "21:08"},
        "bitcoin_price_schedule": {"time": "21:10"},
        "currency_price_schedule": {"time": "21:12"},
        "indices_price_schedule": {"time": "21:14"},
        "special_info_backfill_schedule": {"time": "21:27"},
    }
    global _legacy_coexist_warned
    config = None
    if CONFIG_PATH.exists():
        try:
            config = read_config_file(CONFIG_PATH)
        except OSError as exc:
            # 「讀不到」不等於「內容壞了」（權限、暫時性 IO 錯誤）：**絕不改名隔離**，
            # 否則等於把內容完好的使用者設定靜默搬走。本輪退回預設值，原檔留著，
            # 下次啟動仍可讀回。
            logger.error("讀取設定檔 %s 失敗（保留原檔不動，本輪改用預設值）：%s",
                         CONFIG_PATH, exc)
        except ValueError as exc:
            # 內容真的毀損（JSON 壞掉、非 UTF-8、頂層不是物件）：留著它會讓
            # migrate_legacy_config 認定「新位置已有設定」而永不搬遷，也會讓每次
            # load_config 都拋例外使服務卡在重啟迴圈。先隔離，再給舊設定一次機會。
            logger.error("設定檔 %s 內容毀損：%s", CONFIG_PATH, exc)
            config = None
            if quarantine_corrupt_config():
                # 毀損檔已移走，先前那次「新舊並存」判讀已不成立，重設旗標讓後續
                # 真的並存時仍會告警一次。
                _legacy_coexist_warned = False
                if migrate_legacy_config():
                    try:
                        config = read_config_file(CONFIG_PATH)
                    except (OSError, ValueError) as exc2:
                        logger.error("搬遷後仍無法讀取設定檔 %s：%s",
                                     CONFIG_PATH, exc2)
                        config = None
    if config is not None:
        try:
            return _normalize_config(config, default)
        except (AttributeError, KeyError, TypeError, ValueError,
                RecursionError) as exc:
            # 保險絲：_normalize_config 已把已知的型別／格式錯就地修復，走到這裡
            # 代表出現預期外的結構問題。不攔的話每次 load_config 都會拋例外，
            # --restart always 下就是重啟迴圈，故處置與頂層毀損一致（隔離 + 預設值）。
            logger.error("設定檔 %s 內容不合預期：%s", CONFIG_PATH, exc)
            quarantine_corrupt_config()
    return default


def save_config(config):
    """儲存設定檔至獨立設定目錄（CONFIG_PATH）。

    寫入前確保設定目錄存在：容器可能以尚未建立的 host 路徑掛載設定目錄，
    或執行期被外部刪除，缺目錄時寫入會失敗而讓 Web 介面的修改整個丟失。

    寫入為原子操作（暫存檔 + Path.replace），但**未做 fsync**：正常的容器重啟／
    docker rm -f 不受影響，僅在主機層突然斷電或 VM 崩潰時可能丟失最後一次寫入。

    Args:
        config (dict): 設定內容。

    Raises:
        OSError: 設定目錄無法建立或檔案寫入失敗（呼叫端據此決定是否保留舊檔）。
    """
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    # 原子寫入：先寫同目錄的暫存檔再 os.replace 換上去。直接 open(..., "w") 會先
    # 截斷舊檔，寫到一半失敗（磁碟滿、容器被 kill）就留下毀損 JSON——而毀損的新檔
    # 會讓 migrate_legacy_config 認定「新位置已有設定」而永不再搬遷，等於把使用者
    # 設定連同備援一起弄丟。os.replace 在同一檔案系統上為原子操作。
    # 暫存檔名帶 pid + 遞增序號，確保每次寫入各用各的暫存檔：排程端點是同步 def
    # （跑在 FastAPI threadpool），前端首屏會併發打十幾個 GET，共用固定暫存檔名時
    # 兩個執行緒會互相寫壞對方的暫存檔。（不用 thread id：執行緒結束後 id 會被重用。）
    tmp_path = CONFIG_PATH.with_name(
        f"{CONFIG_PATH.name}.{os.getpid()}.{next(_config_tmp_counter)}.tmp"
    )
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        # Path.replace 即 os.replace，同檔案系統內為原子操作：換上去的一定是完整檔案。
        tmp_path.replace(CONFIG_PATH)
    except Exception:
        # 任何失敗都要清掉暫存檔（含 json.dump 遇不可序列化物件的 TypeError），
        # 避免殘留半截檔案；既有設定檔維持原樣不受影響。
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def setup_schedule(
    schedule_time, tdcc_schedule=None, ctee_schedule=None,
    cnyes_schedule=None, ptt_schedule=None, moneyudn_schedule=None,
    yt_transcript_schedule=None, oil_price_schedule=None,
    gold_price_schedule=None, bitcoin_price_schedule=None,
    currency_price_schedule=None, indices_price_schedule=None,
    special_info_backfill_schedule=None,
):
    """設定每日排程（含各資料來源每日檢查）。

    Args:
        schedule_time (str): 每日資料上傳排程時間，格式為 HH:MM。
        tdcc_schedule (dict | None): TDCC 每日排程設定，
            包含 time（HH:MM）。
        ctee_schedule (dict | None): CTEE 新聞每日排程設定，
            包含 time（HH:MM）。
        cnyes_schedule (dict | None): CNYES 新聞每日排程設定，
            包含 time（HH:MM）。
        ptt_schedule (dict | None): PTT 新聞每日排程設定，
            包含 time（HH:MM）。
        moneyudn_schedule (dict | None): MoneyUDN 新聞每日排程設定，
            包含 time（HH:MM）。
        yt_transcript_schedule (dict | None): YT 逐字稿每日排程設定，
            包含 time（HH:MM）。
        oil_price_schedule (dict | None): 原油價格每日排程設定，
            包含 time（HH:MM）。
        gold_price_schedule (dict | None): 黃金價格每日排程設定，
            包含 time（HH:MM）。
        bitcoin_price_schedule (dict | None): 比特幣價格每日排程設定，
            包含 time（HH:MM）。
        currency_price_schedule (dict | None): 匯率每日排程設定，
            包含 time（HH:MM）。
        indices_price_schedule (dict | None): 股市指數價格每日排程設定，
            包含 time（HH:MM）。
        special_info_backfill_schedule (dict | None): SPECIAL_INFO 每日缺漏
            自我修復偵測補抓排程設定，包含 time（HH:MM）。
    """
    with schedule_lock:
        schedule_lib.clear()
        # 註冊背景執行緒包裝而非 daily_craw 本身，避免長時間爬取阻塞排程執行緒。
        schedule_lib.every().day.at(schedule_time).do(run_daily_craw_scheduled)
        logger.info("每日排程已設定為 %s", schedule_time)

        if tdcc_schedule:
            tdcc_time = tdcc_schedule.get("time", "21:03")
            schedule_lib.every().day.at(tdcc_time).do(
                run_tdcc_scheduled
            )
            logger.info(
                "TDCC 每日排程已設定為 %s", tdcc_time
            )

        if ctee_schedule:
            ctee_time = ctee_schedule.get("time", "21:16")
            schedule_lib.every().day.at(ctee_time).do(
                run_ctee_news_scheduled
            )
            logger.info(
                "CTEE 新聞每日排程已設定為 %s", ctee_time
            )

        if cnyes_schedule:
            cnyes_time = cnyes_schedule.get("time", "21:18")
            schedule_lib.every().day.at(cnyes_time).do(
                run_cnyes_news_scheduled
            )
            logger.info(
                "CNYES 新聞每日排程已設定為 %s", cnyes_time
            )

        if ptt_schedule:
            ptt_time = ptt_schedule.get("time", "21:20")
            schedule_lib.every().day.at(ptt_time).do(
                run_ptt_news_scheduled
            )
            logger.info(
                "PTT 新聞每日排程已設定為 %s", ptt_time
            )

        if moneyudn_schedule:
            moneyudn_time = moneyudn_schedule.get("time", "21:22")
            schedule_lib.every().day.at(moneyudn_time).do(
                run_moneyudn_news_scheduled
            )
            logger.info(
                "MoneyUDN 新聞每日排程已設定為 %s", moneyudn_time
            )

        if yt_transcript_schedule:
            yt_time = yt_transcript_schedule.get("time", "21:24")
            schedule_lib.every().day.at(yt_time).do(
                run_yt_transcript_scheduled
            )
            logger.info("YT 逐字稿每日排程已設定為 %s", yt_time)

        if oil_price_schedule:
            oil_time = oil_price_schedule.get("time", "21:06")
            schedule_lib.every().day.at(oil_time).do(
                run_oil_price_scheduled
            )
            logger.info("原油價格每日排程已設定為 %s", oil_time)

        if gold_price_schedule:
            gold_time = gold_price_schedule.get("time", "21:08")
            schedule_lib.every().day.at(gold_time).do(
                run_gold_price_scheduled
            )
            logger.info("黃金價格每日排程已設定為 %s", gold_time)

        if bitcoin_price_schedule:
            bitcoin_time = bitcoin_price_schedule.get("time", "21:10")
            schedule_lib.every().day.at(bitcoin_time).do(
                run_bitcoin_price_scheduled
            )
            logger.info("比特幣價格每日排程已設定為 %s", bitcoin_time)

        if currency_price_schedule:
            currency_time = currency_price_schedule.get("time", "21:12")
            schedule_lib.every().day.at(currency_time).do(
                run_currency_price_scheduled
            )
            logger.info("匯率每日排程已設定為 %s", currency_time)

        if indices_price_schedule:
            indices_time = indices_price_schedule.get("time", "21:14")
            schedule_lib.every().day.at(indices_time).do(
                run_indices_price_scheduled
            )
            logger.info("股市指數價格每日排程已設定為 %s", indices_time)

        if special_info_backfill_schedule:
            backfill_time = special_info_backfill_schedule.get("time", "21:27")
            schedule_lib.every().day.at(backfill_time).do(
                run_special_info_backfill_scheduled
            )
            logger.info(
                "SPECIAL_INFO 缺漏自我修復每日排程已設定為 %s", backfill_time
            )

        # 每小時執行重試佇列（背景執行緒，不可阻塞排程）
        schedule_lib.every(1).hours.do(run_retry_queue_scheduled)
        logger.info("重試佇列每小時排程已設定。")

        # 每日隔日重排：將未達上限的 exhausted 任務重設為 pending
        schedule_lib.every().day.at(REQUEUE_EXHAUSTED_TIME).do(
            requeue_exhausted_scheduled
        )
        logger.info(
            "exhausted 任務隔日重排已設定為 %s", REQUEUE_EXHAUSTED_TIME
        )


def process_retry_queue():
    """處理重試佇列中的 pending 任務。

    檢查網路連通後，逐一執行 pending 任務。
    成功則標為 success；`SourceError`（爬蟲可達、來源端抓取失敗）維持
    pending 並**續處理其餘任務**；`NetworkError`（爬蟲不可達）則維持
    pending 並中斷本輪；非網路錯誤或超過重試上限則標為 exhausted。
    """
    global retry_queue
    if retry_queue is None:
        return

    pending = retry_queue.get_pending()
    if not pending:
        return

    logger.info("開始處理重試佇列，共 %d 筆 pending 任務。", len(pending))

    if not check_network_available(CRAWLERHOST):
        logger.warning("爬蟲服務不可達，跳過本次重試。")
        return

    for task in pending:
        if task.retry_count >= task.max_retries:
            retry_queue.update_status(task.task_id, "exhausted")
            logger.warning(
                "重試任務 %s 已達上限 %d 次，標為 exhausted。",
                task.task_id, task.max_retries,
            )
            continue

        retry_queue.update_status(task.task_id, "retrying")
        logger.info(
            "重試任務 %s（%s），第 %d 次重試。",
            task.task_id, task.task_type, task.retry_count,
        )

        try:
            _execute_retry_task(task)
            retry_queue.update_status(task.task_id, "success")
            logger.info("重試任務 %s 成功。", task.task_id)
        except SourceError as e:
            # 爬蟲仍可達，只是這筆任務在來源端抓取失敗：維持 pending 待下輪
            # 重試，但**不可中斷整個佇列**，否則單一「毒任務」會癱瘓其後所有
            # 待重試任務（達 max_retries 後才會由迴圈開頭標為 exhausted）。
            logger.warning(
                "重試任務 %s 來源端抓取失敗：%s，續處理其餘任務。",
                task.task_id, e,
            )
            retry_queue.update_status(
                task.task_id, "pending", str(e)
            )
        except NetworkError as e:
            logger.warning(
                "重試任務 %s 仍然網路失敗：%s，中斷本輪重試。",
                task.task_id, e,
            )
            retry_queue.update_status(
                task.task_id, "pending", str(e)
            )
            break
        except Exception as e:
            logger.error(
                "重試任務 %s 非網路錯誤：%s，標為 exhausted。",
                task.task_id, e,
            )
            retry_queue.update_status(
                task.task_id, "exhausted", str(e)
            )


def run_retry_queue_scheduled():
    """觸發重試佇列處理，於獨立背景執行緒執行。

    `process_retry_queue` 逐一同步執行佇列任務，新聞類是整個 48 小時窗重抓，
    單輪耗時可達數分鐘以上。若直接由 `scheduler_thread` 呼叫，會在
    `run_pending()` 期間持有 `schedule_lock`，使其後所有排程延後觸發——這正是
    2026-08 事故的成因。此包裝函式讓排程執行緒立即返回，並以 `retry_queue_running`
    旗標避免上一輪尚未結束時重複啟動（同一任務被兩輪同時執行會重複寫入）。

    Returns:
        bool: 已啟動背景執行緒為 True；上一輪仍在執行或建立執行緒失敗為 False。
    """
    global retry_queue_running

    with retry_queue_lock:
        if retry_queue_running:
            logger.warning("上一輪重試佇列尚未結束，略過本次觸發。")
            return False
        retry_queue_running = True

    def _run():
        """實際執行重試佇列並在結束後釋放重入旗標。"""
        global retry_queue_running
        try:
            process_retry_queue()
        except Exception:  # noqa: BLE001 - 背景執行緒需吞例外避免靜默死亡
            logger.exception("重試佇列處理失敗。")
        finally:
            with retry_queue_lock:
                retry_queue_running = False
            logger.info("重試佇列背景執行緒結束。")

    try:
        threading.Thread(
            target=_run, daemon=True, name="retry-queue"
        ).start()
    except RuntimeError:
        # 無法建立執行緒時必須回復旗標，否則此後每輪重試都只會被略過。
        with retry_queue_lock:
            retry_queue_running = False
        logger.exception("重試佇列背景執行緒建立失敗。")
        return False

    logger.info("重試佇列已於背景執行緒啟動。")
    return True


def requeue_exhausted_scheduled():
    """每日隔日重排：將未達上限的 exhausted 任務重設為 pending。

    針對「資料尚未發布」這類暫時性失敗，隔日資料通常已可取得，
    故每日重排一輪，交由每小時的 process_retry_queue 重新執行。
    達 max_requeues 上限的任務維持 exhausted，視為永久失敗。
    """
    global retry_queue
    if retry_queue is None:
        return

    requeued, kept = retry_queue.requeue_exhausted()
    if requeued or kept:
        logger.info(
            "隔日重排完成：重排 %d 筆 exhausted 任務，%d 筆已達上限維持 exhausted。",
            requeued, kept,
        )


def _execute_retry_task(task):
    """根據任務類型分發執行重試任務。

    Args:
        task (RetryTask): 要重試的任務。

    Raises:
        NetworkError: 網路連線失敗。
        Exception: 其他執行錯誤。
    """
    if task.task_type == "daily_upload":
        db_name = task.params["db_name"]
        dates = task.params["dates"]
        opt = EasyDict({
            "host": HOST,
            "user": USER,
            "password": PASSWORD,
            "dbname": db_name,
            "crawlerhost": CRAWLERHOST,
        })
        for date in sorted(dates):
            pause_duration = random.uniform(3, 15)
            time.sleep(pause_duration)
            day_upload(date, opt)

    elif task.task_type == "ctee_news":
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = CTEENewsUploader(conn, CRAWLERHOST)
            uploader.upload_by_hours(task.params["hours"])

    elif task.task_type == "cnyes_news":
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = CNYESNewsUploader(conn, CRAWLERHOST)
            uploader.upload_by_hours(task.params["hours"])

    elif task.task_type == "ptt_news":
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = PTTNewsUploader(conn, CRAWLERHOST)
            uploader.upload_by_hours(task.params["hours"])

    elif task.task_type == "moneyudn_news":
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = MoneyUDNNewsUploader(conn, CRAWLERHOST)
            uploader.upload_by_hours(task.params["hours"])

    elif task.task_type == "tdcc":
        with db_conn(HOST, USER, PASSWORD, "TWSE") as conn:
            uploader = TDCCUploader(conn, CRAWLERHOST)
            uploader.upload()

    elif task.task_type == "oil_price":
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = OilPriceUploader(conn, CRAWLERHOST)
            date = task.params.get("date")
            if date:
                uploader.upload(date)

    elif task.task_type == "gold_price":
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = GoldPriceUploader(conn, CRAWLERHOST)
            date = task.params.get("date")
            if date:
                uploader.upload(date)

    elif task.task_type == "bitcoin_price":
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = BitcoinPriceUploader(conn, CRAWLERHOST)
            date = task.params.get("date")
            if date:
                uploader.upload(date)

    elif task.task_type == "currency_price":
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = CurrencyPriceUploader(conn, CRAWLERHOST)
            date = task.params.get("date")
            if date:
                uploader.upload(date)

    elif task.task_type == "indices_price":
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = IndicesPriceUploader(conn, CRAWLERHOST)
            date = task.params.get("date")
            if date:
                uploader.upload(date)

    else:
        raise ValueError(f"不支援的重試任務類型：{task.task_type}")


def run_daily_craw_scheduled():
    """排程觸發的每日爬蟲，於獨立背景執行緒執行。

    `daily_craw` 為長時間任務（缺漏天數多或爬蟲回應慢時可達數小時），若直接由
    `scheduler_thread` 同步呼叫，會在 `run_pending()` 期間持有 `schedule_lock`，
    使當日後續排程全部延後。此包裝函式改以背景執行緒執行，讓排程執行緒立即返回；
    並以 `daily_craw_running` 旗標避免上一輪尚未結束時重複啟動。
    """
    global daily_craw_running

    with daily_craw_lock:
        if daily_craw_running:
            logger.warning("上一輪每日爬蟲尚未結束，略過本次排程觸發。")
            return
        daily_craw_running = True

    def _run():
        """實際執行 daily_craw 並在結束後釋放重入旗標。"""
        global daily_craw_running
        try:
            daily_craw()
        except Exception:  # noqa: BLE001 - 背景執行緒需吞例外避免靜默死亡
            logger.exception("每日爬蟲執行失敗。")
        finally:
            with daily_craw_lock:
                daily_craw_running = False
            logger.info("每日爬蟲背景執行緒結束。")

    try:
        threading.Thread(target=_run, daemon=True, name="daily-craw").start()
    except RuntimeError:
        # 無法建立執行緒時必須回復旗標，否則此後每日排程都只會被略過。
        with daily_craw_lock:
            daily_craw_running = False
        logger.exception("每日爬蟲背景執行緒建立失敗。")
        return

    logger.info("每日爬蟲已於背景執行緒啟動。")


def scheduler_thread():
    """排程執行緒，持續檢查並執行待處理的排程任務。"""
    while True:
        with schedule_lock:
            schedule_lib.run_pending()
        time.sleep(1)


def run_upload_job(job_id, start_date, end_date, databases):
    """執行上傳任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期，格式為 YYYY-MM-DD。
        end_date (str): 結束日期，格式為 YYYY-MM-DD。
        databases (list[str]): 資料庫名稱清單。
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    total_tasks = len(dates) * len(databases)

    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"
        upload_jobs[job_id]["total"] = total_tasks
        upload_jobs[job_id]["completed"] = 0

    completed = 0

    try:
        for db_name in databases:
            opt = EasyDict({
                "host": HOST,
                "user": USER,
                "password": PASSWORD,
                "dbname": db_name,
                "crawlerhost": CRAWLERHOST,
            })

            for date in dates:
                with jobs_lock:
                    upload_jobs[job_id]["current_date"] = date
                    upload_jobs[job_id]["current_db"] = db_name

                try:
                    pause_duration = random.uniform(3, 15)
                    time.sleep(pause_duration)
                    day_upload(date, opt)
                except Exception as e:
                    logger.error("上傳失敗 %s %s: %s", db_name, date, e)
                    with jobs_lock:
                        upload_jobs[job_id]["errors"].append(
                            f"{db_name} {date}: {str(e)}"
                        )

                completed += 1
                with jobs_lock:
                    upload_jobs[job_id]["completed"] = completed

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("上傳任務完成 %s", job_id)

    except Exception as e:
        logger.error("上傳任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_ctee_news_scheduled():
    """排程觸發的 CTEE 新聞上傳（過去 48 小時）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "ctee_news",
            "status": "queued",
            "date": today,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_ctee_news_hours_job, (job_id, NEWS_SCHEDULE_HOURS)
    )
    logger.info(
        "CTEE 新聞排程任務已建立 %s（hours=%d）", job_id, NEWS_SCHEDULE_HOURS
    )


def run_ctee_news_upload_job(job_id, start_date, end_date):
    """執行 CTEE 新聞上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的新聞。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = CTEENewsUploader(conn, CRAWLERHOST)

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            total_records = 0
            total_files = 0
            current = start_dt
            failed_dates = []
            unavailable_dates = []

            while current <= end_dt:
                date_str = current.strftime("%Y-%m-%d")
                with jobs_lock:
                    upload_jobs[job_id]["date"] = date_str

                # 單日失敗不得中斷整段回補：逐日隔離例外，最後再彙總回報。
                # 否則首日若超出來源回溯範圍，後面能補的日期會一筆都補不到。
                try:
                    result = uploader.upload(date_str)
                    total_records += result["record_count"]
                    total_files += result["file_count"]
                except OutOfRangeError as e:
                    logger.warning(
                        "CTEE 新聞 %s 超出來源可回溯範圍，略過：%s",
                        date_str, e,
                    )
                    unavailable_dates.append(date_str)
                except SourceError as e:
                    # 抓取不完整：已取得的部分早已落地，須把實際筆數計入總數，
                    # 否則介面顯示 0 筆會被誤判成「完全沒抓到」。該日仍列入
                    # failed_dates 以便重抓補齊剩餘資料。
                    partial = e.partial_result or {}
                    total_records += partial.get("record_count", 0)
                    total_files += partial.get("file_count", 0)
                    logger.warning(
                        "CTEE 新聞 %s 抓取不完整（已存入 %d 筆）：%s",
                        date_str, partial.get("record_count", 0), e,
                    )
                    failed_dates.append(date_str)
                except NetworkError as e:
                    logger.warning(
                        "CTEE 新聞 %s 抓取失敗：%s", date_str, e
                    )
                    failed_dates.append(date_str)
                current += timedelta(days=1)

            with jobs_lock:
                upload_jobs[job_id]["status"] = (
                    "failed" if failed_dates else "completed"
                )
                upload_jobs[job_id]["record_count"] = total_records
                upload_jobs[job_id]["file_count"] = total_files
                if failed_dates:
                    upload_jobs[job_id]["error"] = (
                        "下列日期抓取失敗：" + "、".join(failed_dates)
                    )
                if unavailable_dates:
                    upload_jobs[job_id]["unavailable_dates"] = unavailable_dates
                upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
            logger.info(
                "CTEE 新聞任務結束 %s（%d 筆 metadata，%d 個檔案，"
                "失敗 %d 日、超出回溯範圍 %d 日）",
                job_id, total_records, total_files,
                len(failed_dates), len(unavailable_dates),
            )

    except Exception as e:
        logger.error("CTEE 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_ctee_news_hours_job(job_id, hours):
    """執行 CTEE 新聞時數模式上傳任務（背景執行緒）。

    使用 hours 參數呼叫爬蟲 API，取得過去指定小時數的新聞，
    自動處理跨日資料的去重與上傳。

    Args:
        job_id (str): 任務 ID。
        hours (int): 要回溯的小時數。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = CTEENewsUploader(conn, CRAWLERHOST)

            result = uploader.upload_by_hours(hours)

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["file_count"] = result["file_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "CTEE 新聞任務完成 %s（hours=%d，%d 筆 metadata，%d 個檔案）",
            job_id, hours, result["record_count"], result["file_count"],
        )

    except SourceError as e:
        # 抓取不完整：已取得的部分早已落地，如實回報筆數，
        # 否則介面顯示 0 筆會被誤判成「完全沒抓到」。
        partial = e.partial_result or {}
        logger.warning(
            "CTEE 新聞任務抓取不完整 %s（已存入 %d 筆）：%s",
            job_id, partial.get("record_count", 0), e,
        )
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["record_count"] = partial.get(
                "record_count", 0
            )
            upload_jobs[job_id]["file_count"] = partial.get(
                "file_count", 0
            )
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "ctee_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except NetworkError as e:
        logger.warning("CTEE 新聞任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "ctee_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("CTEE 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_cnyes_news_scheduled():
    """排程觸發的 CNYES 新聞上傳（過去 48 小時）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "cnyes_news",
            "status": "queued",
            "date": today,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_cnyes_news_hours_job, (job_id, NEWS_SCHEDULE_HOURS)
    )
    logger.info(
        "CNYES 新聞排程任務已建立 %s（hours=%d）", job_id, NEWS_SCHEDULE_HOURS
    )


def run_cnyes_news_upload_job(job_id, start_date, end_date):
    """執行 CNYES 新聞上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的新聞。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = CNYESNewsUploader(conn, CRAWLERHOST)

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            total_records = 0
            total_files = 0
            current = start_dt
            failed_dates = []
            unavailable_dates = []

            while current <= end_dt:
                date_str = current.strftime("%Y-%m-%d")
                with jobs_lock:
                    upload_jobs[job_id]["date"] = date_str

                # 單日失敗不得中斷整段回補：逐日隔離例外，最後再彙總回報。
                # 否則首日若超出來源回溯範圍，後面能補的日期會一筆都補不到。
                try:
                    result = uploader.upload(date_str)
                    total_records += result["record_count"]
                    total_files += result["file_count"]
                except OutOfRangeError as e:
                    logger.warning(
                        "CNYES 新聞 %s 超出來源可回溯範圍，略過：%s",
                        date_str, e,
                    )
                    unavailable_dates.append(date_str)
                except SourceError as e:
                    # 抓取不完整：已取得的部分早已落地，須把實際筆數計入總數，
                    # 否則介面顯示 0 筆會被誤判成「完全沒抓到」。該日仍列入
                    # failed_dates 以便重抓補齊剩餘資料。
                    partial = e.partial_result or {}
                    total_records += partial.get("record_count", 0)
                    total_files += partial.get("file_count", 0)
                    logger.warning(
                        "CNYES 新聞 %s 抓取不完整（已存入 %d 筆）：%s",
                        date_str, partial.get("record_count", 0), e,
                    )
                    failed_dates.append(date_str)
                except NetworkError as e:
                    logger.warning(
                        "CNYES 新聞 %s 抓取失敗：%s", date_str, e
                    )
                    failed_dates.append(date_str)
                current += timedelta(days=1)

            with jobs_lock:
                upload_jobs[job_id]["status"] = (
                    "failed" if failed_dates else "completed"
                )
                upload_jobs[job_id]["record_count"] = total_records
                upload_jobs[job_id]["file_count"] = total_files
                if failed_dates:
                    upload_jobs[job_id]["error"] = (
                        "下列日期抓取失敗：" + "、".join(failed_dates)
                    )
                if unavailable_dates:
                    upload_jobs[job_id]["unavailable_dates"] = unavailable_dates
                upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
            logger.info(
                "CNYES 新聞任務結束 %s（%d 筆 metadata，%d 個檔案，"
                "失敗 %d 日、超出回溯範圍 %d 日）",
                job_id, total_records, total_files,
                len(failed_dates), len(unavailable_dates),
            )

    except Exception as e:
        logger.error("CNYES 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_cnyes_news_hours_job(job_id, hours):
    """執行 CNYES 新聞時數模式上傳任務（背景執行緒）。

    使用 hours 參數呼叫爬蟲 API，取得過去指定小時數的新聞，
    自動處理跨日資料的去重與上傳。

    Args:
        job_id (str): 任務 ID。
        hours (int): 要回溯的小時數。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = CNYESNewsUploader(conn, CRAWLERHOST)

            result = uploader.upload_by_hours(hours)

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["file_count"] = result["file_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "CNYES 新聞任務完成 %s（hours=%d，%d 筆 metadata，%d 個檔案）",
            job_id, hours, result["record_count"], result["file_count"],
        )

    except SourceError as e:
        # 抓取不完整：已取得的部分早已落地，如實回報筆數，
        # 否則介面顯示 0 筆會被誤判成「完全沒抓到」。
        partial = e.partial_result or {}
        logger.warning(
            "CNYES 新聞任務抓取不完整 %s（已存入 %d 筆）：%s",
            job_id, partial.get("record_count", 0), e,
        )
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["record_count"] = partial.get(
                "record_count", 0
            )
            upload_jobs[job_id]["file_count"] = partial.get(
                "file_count", 0
            )
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "cnyes_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except NetworkError as e:
        logger.warning("CNYES 新聞任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "cnyes_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("CNYES 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_ptt_news_scheduled():
    """排程觸發的 PTT 新聞上傳（過去 48 小時）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "ptt_news",
            "status": "queued",
            "date": today,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_ptt_news_hours_job, (job_id, NEWS_SCHEDULE_HOURS)
    )
    logger.info(
        "PTT 新聞排程任務已建立 %s（hours=%d）", job_id, NEWS_SCHEDULE_HOURS
    )


def run_ptt_news_upload_job(job_id, start_date, end_date):
    """執行 PTT 新聞上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的新聞。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = PTTNewsUploader(conn, CRAWLERHOST)

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            total_records = 0
            total_files = 0
            current = start_dt
            failed_dates = []
            unavailable_dates = []

            while current <= end_dt:
                date_str = current.strftime("%Y-%m-%d")
                with jobs_lock:
                    upload_jobs[job_id]["date"] = date_str

                # 單日失敗不得中斷整段回補：逐日隔離例外，最後再彙總回報。
                # 否則首日若超出來源回溯範圍，後面能補的日期會一筆都補不到。
                try:
                    result = uploader.upload(date_str)
                    total_records += result["record_count"]
                    total_files += result["file_count"]
                except OutOfRangeError as e:
                    logger.warning(
                        "PTT 新聞 %s 超出來源可回溯範圍，略過：%s",
                        date_str, e,
                    )
                    unavailable_dates.append(date_str)
                except SourceError as e:
                    # 抓取不完整：已取得的部分早已落地，須把實際筆數計入總數，
                    # 否則介面顯示 0 筆會被誤判成「完全沒抓到」。該日仍列入
                    # failed_dates 以便重抓補齊剩餘資料。
                    partial = e.partial_result or {}
                    total_records += partial.get("record_count", 0)
                    total_files += partial.get("file_count", 0)
                    logger.warning(
                        "PTT 新聞 %s 抓取不完整（已存入 %d 筆）：%s",
                        date_str, partial.get("record_count", 0), e,
                    )
                    failed_dates.append(date_str)
                except NetworkError as e:
                    logger.warning(
                        "PTT 新聞 %s 抓取失敗：%s", date_str, e
                    )
                    failed_dates.append(date_str)
                current += timedelta(days=1)

            with jobs_lock:
                upload_jobs[job_id]["status"] = (
                    "failed" if failed_dates else "completed"
                )
                upload_jobs[job_id]["record_count"] = total_records
                upload_jobs[job_id]["file_count"] = total_files
                if failed_dates:
                    upload_jobs[job_id]["error"] = (
                        "下列日期抓取失敗：" + "、".join(failed_dates)
                    )
                if unavailable_dates:
                    upload_jobs[job_id]["unavailable_dates"] = unavailable_dates
                upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
            logger.info(
                "PTT 新聞任務結束 %s（%d 筆 metadata，%d 個檔案，"
                "失敗 %d 日、超出回溯範圍 %d 日）",
                job_id, total_records, total_files,
                len(failed_dates), len(unavailable_dates),
            )

    except Exception as e:
        logger.error("PTT 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_ptt_news_hours_job(job_id, hours):
    """執行 PTT 新聞時數模式上傳任務（背景執行緒）。

    使用 hours 參數呼叫爬蟲 API，取得過去指定小時數的新聞，
    自動處理跨日資料的去重與上傳。

    Args:
        job_id (str): 任務 ID。
        hours (int): 要回溯的小時數。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = PTTNewsUploader(conn, CRAWLERHOST)

            result = uploader.upload_by_hours(hours)

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["file_count"] = result["file_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "PTT 新聞任務完成 %s（hours=%d，%d 筆 metadata，%d 個檔案）",
            job_id, hours, result["record_count"], result["file_count"],
        )

    except SourceError as e:
        # 抓取不完整：已取得的部分早已落地，如實回報筆數，
        # 否則介面顯示 0 筆會被誤判成「完全沒抓到」。
        partial = e.partial_result or {}
        logger.warning(
            "PTT 新聞任務抓取不完整 %s（已存入 %d 筆）：%s",
            job_id, partial.get("record_count", 0), e,
        )
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["record_count"] = partial.get(
                "record_count", 0
            )
            upload_jobs[job_id]["file_count"] = partial.get(
                "file_count", 0
            )
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "ptt_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except NetworkError as e:
        logger.warning("PTT 新聞任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "ptt_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("PTT 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_moneyudn_news_scheduled():
    """排程觸發的 MoneyUDN 新聞上傳（過去 48 小時）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "moneyudn_news",
            "status": "queued",
            "date": today,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_moneyudn_news_hours_job, (job_id, NEWS_SCHEDULE_HOURS)
    )
    logger.info(
        "MoneyUDN 新聞排程任務已建立 %s（hours=%d）",
        job_id, NEWS_SCHEDULE_HOURS,
    )


def run_moneyudn_news_upload_job(job_id, start_date, end_date):
    """執行 MoneyUDN 新聞上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的新聞。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = MoneyUDNNewsUploader(conn, CRAWLERHOST)

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            total_records = 0
            total_files = 0
            current = start_dt
            failed_dates = []
            unavailable_dates = []

            while current <= end_dt:
                date_str = current.strftime("%Y-%m-%d")
                with jobs_lock:
                    upload_jobs[job_id]["date"] = date_str

                # 單日失敗不得中斷整段回補：逐日隔離例外，最後再彙總回報。
                # 否則首日若超出來源回溯範圍，後面能補的日期會一筆都補不到。
                try:
                    result = uploader.upload(date_str)
                    total_records += result["record_count"]
                    total_files += result["file_count"]
                except OutOfRangeError as e:
                    logger.warning(
                        "MoneyUDN 新聞 %s 超出來源可回溯範圍，略過：%s",
                        date_str, e,
                    )
                    unavailable_dates.append(date_str)
                except SourceError as e:
                    # 抓取不完整：已取得的部分早已落地，須把實際筆數計入總數，
                    # 否則介面顯示 0 筆會被誤判成「完全沒抓到」。該日仍列入
                    # failed_dates 以便重抓補齊剩餘資料。
                    partial = e.partial_result or {}
                    total_records += partial.get("record_count", 0)
                    total_files += partial.get("file_count", 0)
                    logger.warning(
                        "MoneyUDN 新聞 %s 抓取不完整（已存入 %d 筆）：%s",
                        date_str, partial.get("record_count", 0), e,
                    )
                    failed_dates.append(date_str)
                except NetworkError as e:
                    logger.warning(
                        "MoneyUDN 新聞 %s 抓取失敗：%s", date_str, e
                    )
                    failed_dates.append(date_str)
                current += timedelta(days=1)

            with jobs_lock:
                upload_jobs[job_id]["status"] = (
                    "failed" if failed_dates else "completed"
                )
                upload_jobs[job_id]["record_count"] = total_records
                upload_jobs[job_id]["file_count"] = total_files
                if failed_dates:
                    upload_jobs[job_id]["error"] = (
                        "下列日期抓取失敗：" + "、".join(failed_dates)
                    )
                if unavailable_dates:
                    upload_jobs[job_id]["unavailable_dates"] = unavailable_dates
                upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
            logger.info(
                "MoneyUDN 新聞任務結束 %s（%d 筆 metadata，%d 個檔案，"
                "失敗 %d 日、超出回溯範圍 %d 日）",
                job_id, total_records, total_files,
                len(failed_dates), len(unavailable_dates),
            )

    except Exception as e:
        logger.error("MoneyUDN 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_moneyudn_news_hours_job(job_id, hours):
    """執行 MoneyUDN 新聞時數模式上傳任務（背景執行緒）。

    使用 hours 參數呼叫爬蟲 API，取得過去指定小時數的新聞，
    自動處理跨日資料的去重與上傳。

    Args:
        job_id (str): 任務 ID。
        hours (int): 要回溯的小時數。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = MoneyUDNNewsUploader(conn, CRAWLERHOST)

            result = uploader.upload_by_hours(hours)

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["file_count"] = result["file_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "MoneyUDN 新聞任務完成 %s（hours=%d，%d 筆 metadata，%d 個檔案）",
            job_id, hours, result["record_count"], result["file_count"],
        )

    except SourceError as e:
        # 抓取不完整：已取得的部分早已落地，如實回報筆數，
        # 否則介面顯示 0 筆會被誤判成「完全沒抓到」。
        partial = e.partial_result or {}
        logger.warning(
            "MoneyUDN 新聞任務抓取不完整 %s（已存入 %d 筆）：%s",
            job_id, partial.get("record_count", 0), e,
        )
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["record_count"] = partial.get(
                "record_count", 0
            )
            upload_jobs[job_id]["file_count"] = partial.get(
                "file_count", 0
            )
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "moneyudn_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except NetworkError as e:
        logger.warning("MoneyUDN 新聞任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "moneyudn_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("MoneyUDN 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_tdcc_scheduled():
    """排程觸發的 TDCC 上傳。"""
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "tdcc",
            "status": "queued",
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(job_id, run_tdcc_upload_job, (job_id,))
    logger.info("TDCC 排程任務已建立 %s", job_id)


def run_tdcc_upload_job(job_id):
    """執行 TDCC 上傳任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "TWSE") as conn:
            uploader = TDCCUploader(conn, CRAWLERHOST)
            result = uploader.upload()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["date"] = result["date"]
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("TDCC 任務完成 %s", job_id)

    except NetworkError as e:
        logger.warning("TDCC 任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "tdcc", {}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("TDCC 任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_company_info_upload_job(job_id):
    """執行公司產業對照上傳任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "TWSE") as conn:
            uploader = CompanyInfoUploader(conn, CRAWLERHOST)
            result = uploader.upload()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["company_info_count"] = result[
                "company_info_count"
            ]
            upload_jobs[job_id]["industry_map_count"] = result[
                "industry_map_count"
            ]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("公司產業對照任務完成 %s", job_id)

    except Exception as e:
        logger.error("公司產業對照任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def yt_transcript_target_date(schedule_time=None, now=None):
    """計算 YT 逐字稿排程本次應抓的日期（＝排程日的前一日）。

    v3 把排程搬到 21:24，距午夜只剩 2 小時 36 分（舊制 07:54 距午夜 16 小時）。
    宿主休眠時 schedule 套件會在下一次 run_pending() 補跑過期任務；補跑若落到
    隔日凌晨，直接用 `now - 1 天` 會算成「排程日當天」而非「排程日的前一日」，
    使原本該抓的那天被靜默跳過——YT 排程只吃單一日期、**沒有多日回補路徑**，
    漏掉就永久漏掉（行情有 30 天補抓窗、新聞有 48 小時窗可自癒，故不受影響）。

    因此改由「現在的時刻是否已過排定時刻」還原排程日：時刻尚未到，代表這是
    前一晚延遲補跑的任務，排程日往前推一天。與 Tw_stock_crawer 的「YT 抓昨天」
    契約維持一致（準時執行時結果與 `now - 1 天` 完全相同）。

    界線：本函式只還原「單次過期任務」的排程日。宿主連睡超過 24 小時時
    `schedule` 套件本身也只補跑一次，中間那天仍會漏——那是套件限制，需人工
    以 Web 介面補抓。

    Args:
        schedule_time (str | None): 排定時刻（HH:MM），預設讀設定檔。
        now (datetime.datetime | None): 基準時間，預設為當下（供測試注入）。

    Returns:
        str: 應抓取的日期字串（YYYY-MM-DD）。
    """
    base = now or datetime.now()
    if schedule_time is None:
        entry = load_config().get("yt_transcript_schedule", {})
        schedule_time = entry.get("time") if isinstance(entry, dict) else None

    run_date = base.date()
    now_hm = base.strftime("%H:%M")
    if _is_valid_time(schedule_time) and now_hm < schedule_time:
        # 尚未到排定時刻 → 本次是前一晚過期任務的補跑，排程日往前推一天。
        run_date -= timedelta(days=1)
    return (run_date - timedelta(days=1)).strftime("%Y-%m-%d")


def run_yt_transcript_scheduled():
    """排程觸發的 YT 逐字稿上傳（抓排程日的前一日影片）。

    排程於晚間（21:24）執行，此時「當日」直播多半尚未結束或自動字幕尚未
    產生，故抓已完成的前一日直播影片，確保逐字稿已可取得。日期由
    yt_transcript_target_date 計算，可正確處理跨午夜的延遲補跑。
    """
    yesterday = yt_transcript_target_date()
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "yt_transcript",
            "status": "queued",
            "date": yesterday,
            "title": None,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_yt_transcript_upload_job, (job_id, yesterday)
    )
    logger.info("YT 逐字稿排程任務已建立 %s（date=%s）", job_id, yesterday)


def run_yt_transcript_upload_job(job_id, date):
    """執行 YT 逐字稿上傳任務（背景執行緒）。

    Args:
        job_id: 任務 ID。
        date: 日期字串（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            uploader = YTTranscriptUploader(conn)
            result = uploader.upload(date)

        with jobs_lock:
            upload_jobs[job_id]["status"] = (
                "completed" if result["status"] in ("success", "skipped")
                else "failed"
            )
            upload_jobs[job_id]["title"] = result.get("title")
            if result.get("error"):
                upload_jobs[job_id]["error"] = result["error"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("YT 逐字稿任務完成 %s (%s)", job_id, result["status"])

    except Exception as e:
        logger.error("YT 逐字稿任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def settled_end_date(now=None):
    """回傳排程行情抓取的區間上界：昨日（YYYY-MM-DD）。

    SPECIAL_INFO 五商品的日 K 由 yfinance 供應，「當日」那一根在該市場收盤前
    一直是**進行中的半根 K**。舊排程 07:3x（= 前一日 23:3x UTC / 19:3x ET）時
    UTC 尚未跨日、美股現貨也尚未開盤，靠「爬蟲 fallback 回上一交易日」恰好
    只會取到已定案的日 K；v3 搬到 21:0x（= 13:0x UTC / 09:0x ET）後，比特幣與
    匯率當日的 UTC 日 K 已存在且僅完成約一半，會被 REPLACE INTO 寫進價格表並
    記帳，而**帳本與價格表雙重跳過**使該日永遠不會被重驗，等於把半根 K 永久
    凍結。原油／黃金為 CME Globex 期貨（前一日 18:00 ET 開盤），此問題在舊排
    程就已存在，本次一併修掉。

    因此排程一律只抓到昨日為止：當日資料於次日排程自然補上。**資料新鮮度與舊
    制相同**——舊制在 D 日請求 D、實際寫入的也是 D-1 那一根。

    Args:
        now (datetime.datetime | None): 基準時間，預設為當下（供測試注入）。

    Returns:
        str: 昨日日期字串（YYYY-MM-DD）。
    """
    base = now or datetime.now()
    return (base - timedelta(days=1)).strftime("%Y-%m-%d")


def _set_job_date(job_id, date_str):
    """更新任務目前處理中的日期（供 UI 進度顯示與失敗時的重試標記）。

    Args:
        job_id (str): 任務 ID。
        date_str (str): 目前處理的日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["date"] = date_str


def _finish_price_job(job_id, task_type, label, total_records, failures):
    """收尾行情上傳任務：更新狀態並把逐日失敗排入重試佇列。

    來源端失敗（`SourceError`）已在 `upload_date_range` 逐日隔離，不會中斷
    整個區間，故這裡可能同時有「已寫入的筆數」與「失敗的日期」。有失敗時
    狀態記為 `completed_with_errors`，避免部分成功被讀成全數成功——這正是
    舊版「失敗被記成當日無資料」能長期無人察覺的原因之一。

    Args:
        job_id (str): 任務 ID。
        task_type (str): 重試佇列任務類型（如 "oil_price"）。
        label (str): 商品中文名稱，供 log 使用。
        total_records (int): 成功寫入的總筆數。
        failures (list[dict]): 失敗日期清單（含 date 與 error）。
    """
    with jobs_lock:
        job = upload_jobs[job_id]
        job["status"] = "completed" if not failures else "completed_with_errors"
        job["record_count"] = total_records
        job["errors"] = [f"{f['date']}: {f['error']}" for f in failures]
        job["finished_at"] = datetime.now().isoformat()

    if failures and retry_queue is not None:
        for failure in failures:
            retry_queue.add(
                task_type,
                {"date": failure["date"]},
                failure["error"],
                created_by_job_id=job_id,
            )

    logger.info(
        "%s任務完成 %s（共 %d 筆，失敗 %d 天）",
        label, job_id, total_records, len(failures),
    )


def run_oil_price_scheduled():
    """排程觸發的原油價格上傳（過去 7 天補抓，上界為昨日）。"""
    job_id = str(uuid.uuid4())[:8]
    # 只抓到昨日為止：當日日 K 尚未定案，寫入後會被永久凍結
    # （見 settled_end_date）。
    end_date = settled_end_date()

    # 補抓過去 7 天（美國市場可能有延遲）
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "oil_price",
            "status": "queued",
            "start_date": start_date,
            "end_date": end_date,
            "date": end_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_oil_price_upload_job,
        (job_id, start_date, end_date),
    )
    logger.info(
        "原油價格排程任務已建立 %s（%s ~ %s）",
        job_id, start_date, end_date,
    )


def run_oil_price_upload_job(job_id, start_date, end_date):
    """執行原油價格上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的資料。單日的來源端失敗（SourceError）
    與該日的資料格式異常（CrawlError）都只跳過該日並排入重試佇列，不中斷
    整個區間；只有連不上爬蟲（NetworkError）才中止整批
    （見 special_info_common.upload_date_range）。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = OilPriceUploader(conn, CRAWLERHOST)
            outcome = special_info_common.upload_date_range(
                uploader, start_date, end_date,
                on_date=lambda d: _set_job_date(job_id, d),
            )

        _finish_price_job(
            job_id, "oil_price", "原油價格",
            outcome["record_count"], outcome["failures"],
        )

    except CrawlError as e:
        logger.warning("原油價格任務爬取失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "oil_price",
                {"date": upload_jobs[job_id].get("date", end_date)},
                str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("原油價格任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_gold_price_scheduled():
    """排程觸發的黃金價格上傳（過去 7 天補抓，上界為昨日）。"""
    job_id = str(uuid.uuid4())[:8]
    # 只抓到昨日為止：當日日 K 尚未定案，寫入後會被永久凍結
    # （見 settled_end_date）。
    end_date = settled_end_date()

    # 補抓過去 7 天（美國市場可能有延遲）
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "gold_price",
            "status": "queued",
            "start_date": start_date,
            "end_date": end_date,
            "date": end_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_gold_price_upload_job,
        (job_id, start_date, end_date),
    )
    logger.info(
        "黃金價格排程任務已建立 %s（%s ~ %s）",
        job_id, start_date, end_date,
    )


def run_gold_price_upload_job(job_id, start_date, end_date):
    """執行黃金價格上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的資料。單日的來源端失敗（SourceError）
    與該日的資料格式異常（CrawlError）都只跳過該日並排入重試佇列，不中斷
    整個區間；只有連不上爬蟲（NetworkError）才中止整批
    （見 special_info_common.upload_date_range）。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = GoldPriceUploader(conn, CRAWLERHOST)
            outcome = special_info_common.upload_date_range(
                uploader, start_date, end_date,
                on_date=lambda d: _set_job_date(job_id, d),
            )

        _finish_price_job(
            job_id, "gold_price", "黃金價格",
            outcome["record_count"], outcome["failures"],
        )

    except CrawlError as e:
        logger.warning("黃金價格任務爬取失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "gold_price",
                {"date": upload_jobs[job_id].get("date", end_date)},
                str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("黃金價格任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_bitcoin_price_scheduled():
    """排程觸發的比特幣價格上傳（過去 7 天補抓，上界為昨日）。"""
    job_id = str(uuid.uuid4())[:8]
    # 只抓到昨日為止：當日日 K 尚未定案，寫入後會被永久凍結
    # （見 settled_end_date）。
    end_date = settled_end_date()

    # 補抓過去 7 天
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "bitcoin_price",
            "status": "queued",
            "start_date": start_date,
            "end_date": end_date,
            "date": end_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_bitcoin_price_upload_job,
        (job_id, start_date, end_date),
    )
    logger.info(
        "比特幣價格排程任務已建立 %s（%s ~ %s）",
        job_id, start_date, end_date,
    )


def run_bitcoin_price_upload_job(job_id, start_date, end_date):
    """執行比特幣價格上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的資料。單日的來源端失敗（SourceError）
    與該日的資料格式異常（CrawlError）都只跳過該日並排入重試佇列，不中斷
    整個區間；只有連不上爬蟲（NetworkError）才中止整批
    （見 special_info_common.upload_date_range）。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = BitcoinPriceUploader(conn, CRAWLERHOST)
            outcome = special_info_common.upload_date_range(
                uploader, start_date, end_date,
                on_date=lambda d: _set_job_date(job_id, d),
            )

        _finish_price_job(
            job_id, "bitcoin_price", "比特幣價格",
            outcome["record_count"], outcome["failures"],
        )

    except CrawlError as e:
        logger.warning("比特幣價格任務爬取失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "bitcoin_price",
                {"date": upload_jobs[job_id].get("date", end_date)},
                str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("比特幣價格任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_currency_price_scheduled():
    """排程觸發的匯率上傳（過去 7 天補抓，上界為昨日）。"""
    job_id = str(uuid.uuid4())[:8]
    # 只抓到昨日為止：當日日 K 尚未定案，寫入後會被永久凍結
    # （見 settled_end_date）。
    end_date = settled_end_date()

    # 補抓過去 7 天
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "currency_price",
            "status": "queued",
            "start_date": start_date,
            "end_date": end_date,
            "date": end_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_currency_price_upload_job,
        (job_id, start_date, end_date),
    )
    logger.info(
        "匯率排程任務已建立 %s（%s ~ %s）",
        job_id, start_date, end_date,
    )


def run_currency_price_upload_job(job_id, start_date, end_date):
    """執行匯率上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的資料。單日的來源端失敗（SourceError）
    與該日的資料格式異常（CrawlError）都只跳過該日並排入重試佇列，不中斷
    整個區間；只有連不上爬蟲（NetworkError）才中止整批
    （見 special_info_common.upload_date_range）。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = CurrencyPriceUploader(conn, CRAWLERHOST)
            outcome = special_info_common.upload_date_range(
                uploader, start_date, end_date,
                on_date=lambda d: _set_job_date(job_id, d),
            )

        _finish_price_job(
            job_id, "currency_price", "匯率",
            outcome["record_count"], outcome["failures"],
        )

    except CrawlError as e:
        logger.warning("匯率任務爬取失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "currency_price",
                {"date": upload_jobs[job_id].get("date", end_date)},
                str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("匯率任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_indices_price_scheduled():
    """排程觸發的股市指數價格上傳（過去 7 天補抓，上界為昨日）。"""
    job_id = str(uuid.uuid4())[:8]
    # 只抓到昨日為止：當日日 K 尚未定案，寫入後會被永久凍結
    # （見 settled_end_date）。
    end_date = settled_end_date()

    # 補抓過去 7 天（美國市場可能有延遲）
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "indices_price",
            "status": "queued",
            "start_date": start_date,
            "end_date": end_date,
            "date": end_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_indices_price_upload_job,
        (job_id, start_date, end_date),
    )
    logger.info(
        "股市指數價格排程任務已建立 %s（%s ~ %s）",
        job_id, start_date, end_date,
    )


def run_indices_price_upload_job(job_id, start_date, end_date):
    """執行股市指數價格上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的資料。單日的來源端失敗（SourceError）
    與該日的資料格式異常（CrawlError）都只跳過該日並排入重試佇列，不中斷
    整個區間；只有連不上爬蟲（NetworkError）才中止整批
    （見 special_info_common.upload_date_range）。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            uploader = IndicesPriceUploader(conn, CRAWLERHOST)
            outcome = special_info_common.upload_date_range(
                uploader, start_date, end_date,
                on_date=lambda d: _set_job_date(job_id, d),
            )

        _finish_price_job(
            job_id, "indices_price", "股市指數價格",
            outcome["record_count"], outcome["failures"],
        )

    except CrawlError as e:
        logger.warning("股市指數價格任務爬取失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "indices_price",
                {"date": upload_jobs[job_id].get("date", end_date)},
                str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("股市指數價格任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_special_info_backfill_scheduled():
    """排程觸發的 SPECIAL_INFO 缺漏自我修復偵測補抓（近 30 天，上界為昨日）。

    掃描基準日固定為昨日：21:27 執行時當日日 K 尚未定案，若讓補抓把「今日」
    也當成候選，會把半根 K 寫進價格表並記帳而永久凍結（見 settled_end_date）。

    同時重驗最近 SPECIAL_INFO_REVERIFY_DAYS 天的孤兒帳本，讓誤標在當週自癒。
    """
    job_id = str(uuid.uuid4())[:8]
    now = datetime.now().isoformat()
    end_date = settled_end_date()

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "special_info_backfill",
            "status": "queued",
            "days": SPECIAL_INFO_BACKFILL_DAYS,
            "end_date": end_date,
            "record_count": 0,
            "summary": [],
            "errors": [],
            "created_at": now,
            "finished_at": None,
            "scheduled": True,
        }

    job_queue.enqueue(
        job_id, run_special_info_backfill_job,
        (job_id, SPECIAL_INFO_BACKFILL_DAYS, False, end_date,
         SPECIAL_INFO_REVERIFY_DAYS),
    )
    logger.info(
        "SPECIAL_INFO 缺漏自我修復任務已建立 %s（近 %d 天，至 %s，重驗近 %d 天）",
        job_id, SPECIAL_INFO_BACKFILL_DAYS, end_date,
        SPECIAL_INFO_REVERIFY_DAYS,
    )


def run_special_info_backfill_job(
    job_id, days=SPECIAL_INFO_BACKFILL_DAYS, deep=False, today=None,
    reverify_days=0,
):
    """執行 SPECIAL_INFO 缺漏自我修復偵測補抓任務（背景執行緒）。

    對 5 個商品各自掃描近 N 天缺漏並以「問爬蟲」為交易日唯一真相回補。
    冪等、可重跑。逐商品建立獨立連線；某商品失敗不影響其他商品。
    掃描過程中遇 NetworkError 的日期改交由 retry_queue 後續重試；
    CrawlError（格式／型別異常）重試無用，改列入 errors 讓管理介面看得見，
    任務狀態記為 completed_with_errors（其餘商品／日期仍已補齊）。

    Args:
        job_id (str): 任務 ID。
        days (int): 掃描天數，預設 SPECIAL_INFO_BACKFILL_DAYS。
        deep (bool): 是否深度重驗（先清孤兒帳本再重驗），預設 False；
            日常排程用 False，人工修復歷史缺漏用 True。
        today (str | None): 掃描基準日（含），預設當日；排程固定傳昨日，
            避免把尚未定案的當日日 K 寫死（見 settled_end_date）。
        reverify_days (int): deep=False 時要清除孤兒帳本並重驗的天數，
            預設 0（不清）；排程固定傳 SPECIAL_INFO_REVERIFY_DAYS，讓誤標的
            帳本在當週自癒，不必等人工 deep 重驗。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    total_records = 0
    summaries = []
    errors = []
    date_errors = []

    for task_type, uploader_cls in SPECIAL_INFO_ASSETS:
        try:
            with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
                uploader = uploader_cls(conn, CRAWLERHOST)
                summary = uploader.backfill_missing(
                    days=days, today=today, deep=deep,
                    reverify_days=reverify_days,
                )
                total_records += summary["records"]
                summaries.append(summary)

                # 網路失敗的日期交由 retry_queue 後續重試（沿用既有機制）。
                if retry_queue is not None:
                    for date_str in summary["network_errors"]:
                        retry_queue.add(
                            task_type,
                            {"date": date_str},
                            "缺漏自我修復網路失敗",
                            created_by_job_id=job_id,
                        )
                # 格式／型別異常重試多半無用，但必須看得見，否則等同靜默吞掉。
                for date_str in summary.get("crawl_errors", []):
                    date_errors.append(f"{task_type} {date_str}: 爬蟲回傳格式異常")
        except Exception as e:  # noqa: BLE001 逐商品隔離，避免單一失敗中斷全部
            logger.error(
                "SPECIAL_INFO 缺漏自我修復 %s 失敗：%s", task_type, e
            )
            errors.append(f"{task_type}: {e}")

    if errors:
        # 整個商品掛掉（如連不上 DB）：整體視為失敗。
        status = "failed"
    elif date_errors:
        # 只有個別日期格式異常：其餘商品／日期都已補齊，不應讀成全數失敗。
        status = "completed_with_errors"
    else:
        status = "completed"

    with jobs_lock:
        upload_jobs[job_id]["status"] = status
        upload_jobs[job_id]["record_count"] = total_records
        upload_jobs[job_id]["summary"] = summaries
        upload_jobs[job_id]["errors"] = errors + date_errors
        upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()

    logger.info(
        "SPECIAL_INFO 缺漏自我修復任務完成 %s（補回 %d 筆，狀態 %s，"
        "商品失敗 %d 個、日期異常 %d 個）",
        job_id, total_records, status, len(errors), len(date_errors),
    )


# Pydantic 請求模型
class UploadRequest(BaseModel):
    """手動上傳請求。"""
    start_date: str
    end_date: str
    databases: list[str]


class QuarterRevenueRequest(BaseModel):
    """季度營業收入抓取請求。"""
    year: int
    quarter: int


class ScheduleRequest(BaseModel):
    """排程時間更新請求。"""
    time: str


class TDCCScheduleRequest(BaseModel):
    """TDCC 每日排程更新請求。"""
    time: str


class CTEENewsUploadRequest(BaseModel):
    """CTEE 新聞上傳請求。"""
    start_date: str
    end_date: str


class CTEENewsScheduleRequest(BaseModel):
    """CTEE 新聞每日排程更新請求。"""
    time: str


class CNYESNewsUploadRequest(BaseModel):
    """CNYES 新聞上傳請求。"""
    start_date: str
    end_date: str


class CNYESNewsScheduleRequest(BaseModel):
    """CNYES 新聞每日排程更新請求。"""
    time: str


class PTTNewsUploadRequest(BaseModel):
    """PTT 新聞上傳請求。"""
    start_date: str
    end_date: str


class PTTNewsScheduleRequest(BaseModel):
    """PTT 新聞每日排程更新請求。"""
    time: str


class MoneyUDNNewsUploadRequest(BaseModel):
    """MoneyUDN 新聞上傳請求。"""
    start_date: str
    end_date: str


class MoneyUDNNewsScheduleRequest(BaseModel):
    """MoneyUDN 新聞每日排程更新請求。"""
    time: str


class YTTranscriptUploadRequest(BaseModel):
    """YT 逐字稿上傳請求。"""
    date: str


class YTTranscriptScheduleRequest(BaseModel):
    """YT 逐字稿每日排程更新請求。"""
    time: str


class OilPriceUploadRequest(BaseModel):
    """原油價格上傳請求。"""
    start_date: str
    end_date: str


class OilPriceScheduleRequest(BaseModel):
    """原油價格每日排程更新請求。"""
    time: str


class GoldPriceUploadRequest(BaseModel):
    """黃金價格上傳請求。"""
    start_date: str
    end_date: str


class GoldPriceScheduleRequest(BaseModel):
    """黃金價格每日排程更新請求。"""
    time: str


class BitcoinPriceUploadRequest(BaseModel):
    """比特幣價格上傳請求。"""
    start_date: str
    end_date: str


class BitcoinPriceScheduleRequest(BaseModel):
    """比特幣價格每日排程更新請求。"""
    time: str


class CurrencyPriceUploadRequest(BaseModel):
    """匯率上傳請求。"""
    start_date: str
    end_date: str


class CurrencyPriceScheduleRequest(BaseModel):
    """匯率每日排程更新請求。"""
    time: str


class IndicesPriceUploadRequest(BaseModel):
    """股市指數價格上傳請求。"""
    start_date: str
    end_date: str


class IndicesPriceScheduleRequest(BaseModel):
    """股市指數價格每日排程更新請求。"""
    time: str


class SpecialInfoBackfillScheduleRequest(BaseModel):
    """SPECIAL_INFO 缺漏自我修復每日排程更新請求。"""
    time: str


class SpecialInfoBackfillRunRequest(BaseModel):
    """SPECIAL_INFO 缺漏自我修復手動觸發請求。"""
    days: int = SPECIAL_INFO_BACKFILL_DAYS
    deep: bool = False


# FastAPI 應用
@asynccontextmanager
async def lifespan(app: FastAPI):
    """應用程式生命週期管理。"""
    global retry_queue, job_queue
    retry_queue = RetryQueue(LOG_DIR / "retry_queue.json")
    set_retry_queue(retry_queue)
    logger.info("重試佇列已初始化。")

    job_queue = JobQueue(upload_jobs, jobs_lock)
    consumer_thread = threading.Thread(
        target=job_queue.consumer_loop, daemon=True
    )
    consumer_thread.start()
    logger.info("任務佇列已初始化。")

    config = load_config()
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    t = threading.Thread(target=scheduler_thread, daemon=True)
    t.start()
    logger.info("Web 伺服器與排程服務已啟動。")

    yield


# root_path 用於 Dashboard 反向代理（/app/db-operating），讓 FastAPI 知悉外部前綴
# 以便 OpenAPI/Swagger/redirect URL 產生時帶上正確的 base path。
# 若 ROOT_PATH 環境變數未設定，則預設為空字串（直接存取 port 8080）。
app = FastAPI(
    title="台股資料管理介面",
    lifespan=lifespan,
    root_path=os.environ.get("ROOT_PATH", ""),
)


@app.post("/api/upload")
def create_upload(req: UploadRequest):
    """建立手動上傳任務。

    Args:
        req: 包含起始日期、結束日期、資料庫清單的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證資料庫名稱
    for db in req.databases:
        if db not in DB_NAMES:
            raise HTTPException(400, f"不支援的資料庫: {db}")

    if not req.databases:
        raise HTTPException(400, "請至少選擇一個資料庫")

    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "databases": req.databases,
            "total": 0,
            "completed": 0,
            "current_date": "",
            "current_db": "",
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_upload_job,
        (job_id, req.start_date, req.end_date, req.databases),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/upload/jobs")
def list_upload_jobs():
    """列出所有上傳任務。

    Returns:
        list[dict]: 所有任務的狀態資訊。
    """
    with jobs_lock:
        return list(upload_jobs.values())


@app.get("/api/upload/status/{job_id}")
def get_upload_status(job_id: str):
    """查詢上傳任務狀態。

    Args:
        job_id: 任務 ID。

    Returns:
        dict: 任務狀態資訊。
    """
    with jobs_lock:
        if job_id not in upload_jobs:
            raise HTTPException(404, "任務不存在")
        return upload_jobs[job_id]


@app.get("/api/schedule")
def get_schedule():
    """取得目前排程時間。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    return {"time": config["schedule_time"]}


@app.put("/api/schedule")
def update_schedule(req: ScheduleRequest):
    """更新排程時間。

    Args:
        req: 包含新排程時間的請求。

    Returns:
        dict: 更新後的排程時間與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["schedule_time"] = req.time
    save_config(config)
    setup_schedule(
        req.time,
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("排程時間已更新為 %s", req.time)
    return {"time": req.time, "message": f"排程時間已更新為 {req.time}"}


@app.get("/api/databases")
def list_databases():
    """列出可用的資料庫。

    Returns:
        dict: 包含 databases 欄位的資料庫清單。
    """
    return {"databases": DB_NAMES}


def run_quarter_revenue_job(job_id, year, quarter):
    """執行季度營業收入抓取任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
        year (int): 民國年。
        quarter (int): 季度（1-4）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        with db_conn(HOST, USER, PASSWORD, "TWSE") as conn:
            uploader = QuarterRevenueUploader(conn)
            record_count = uploader.upload(year, quarter)

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = record_count
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("季度營業收入任務完成 %s", job_id)

    except Exception as e:
        logger.error("季度營業收入任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


@app.post("/api/quarter-revenue/upload")
def create_quarter_revenue_upload(req: QuarterRevenueRequest):
    """建立季度營業收入抓取任務。

    Args:
        req: 包含年份與季度的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    if req.quarter not in (1, 2, 3, 4):
        raise HTTPException(400, "季度必須為 1-4")

    if not (80 <= req.year <= 200):
        raise HTTPException(400, "年份必須為 80-200（民國年）")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "quarter_revenue",
            "status": "queued",
            "year": req.year,
            "quarter": req.quarter,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_quarter_revenue_job,
        (job_id, req.year, req.quarter),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/quarter-revenue/uploaded")
def list_uploaded_quarters():
    """列出已上傳的季度營業收入記錄。

    Returns:
        dict: 包含 uploaded 欄位的已上傳記錄清單。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "TWSE") as conn:
            rows = conn.execute(
                text(
                    "SELECT Year, Quarter "
                    "FROM QuarterRevenueUploaded "
                    "ORDER BY Year DESC, Quarter DESC"
                )
            ).fetchall()

        uploaded = [
            {
                "year": row[0],
                "quarter": row[1],
            }
            for row in rows
        ]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳季度失敗：%s", e)
        return {"uploaded": []}


@app.post("/api/tdcc/upload")
def create_tdcc_upload():
    """建立 TDCC 上傳任務。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "tdcc",
            "status": "queued",
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_tdcc_upload_job, (job_id,),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/tdcc/uploaded")
def list_uploaded_tdcc():
    """列出已上傳的 TDCC 日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 20 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "TWSE") as conn:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT Date FROM TDCC "
                    "ORDER BY Date DESC LIMIT 20"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 TDCC 日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/tdcc/schedule")
def get_tdcc_schedule():
    """取得 TDCC 每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    tdcc = config.get("tdcc_schedule", {"time": "21:03"})
    return {"time": tdcc["time"]}


@app.put("/api/tdcc/schedule")
def update_tdcc_schedule(req: TDCCScheduleRequest):
    """更新 TDCC 每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["tdcc_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config["tdcc_schedule"],
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("TDCC 每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"TDCC 每日排程已更新為 {req.time}",
    }


# CTEE 新聞 API 端點
@app.post("/api/ctee-news/upload")
def create_ctee_news_upload(req: CTEENewsUploadRequest):
    """建立 CTEE 新聞上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "ctee_news",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_ctee_news_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/ctee-news/uploaded")
def list_uploaded_ctee_news():
    """列出已上傳的 CTEE 新聞日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM CTEEUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 CTEE 新聞日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/ctee-news/schedule")
def get_ctee_news_schedule():
    """取得 CTEE 新聞每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    ctee = config.get("ctee_schedule", {"time": "21:16"})
    return {"time": ctee["time"]}


@app.put("/api/ctee-news/schedule")
def update_ctee_news_schedule(req: CTEENewsScheduleRequest):
    """更新 CTEE 新聞每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["ctee_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config["ctee_schedule"],
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("CTEE 新聞每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"CTEE 新聞每日排程已更新為 {req.time}",
    }


# CNYES 新聞 API 端點
@app.post("/api/cnyes-news/upload")
def create_cnyes_news_upload(req: CNYESNewsUploadRequest):
    """建立 CNYES 新聞上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "cnyes_news",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_cnyes_news_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/cnyes-news/uploaded")
def list_uploaded_cnyes_news():
    """列出已上傳的 CNYES 新聞日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM CNYESUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 CNYES 新聞日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/cnyes-news/schedule")
def get_cnyes_news_schedule():
    """取得 CNYES 新聞每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    cnyes = config.get("cnyes_schedule", {"time": "21:18"})
    return {"time": cnyes["time"]}


@app.put("/api/cnyes-news/schedule")
def update_cnyes_news_schedule(req: CNYESNewsScheduleRequest):
    """更新 CNYES 新聞每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["cnyes_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config["cnyes_schedule"],
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("CNYES 新聞每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"CNYES 新聞每日排程已更新為 {req.time}",
    }


# PTT 新聞 API 端點
@app.post("/api/ptt-news/upload")
def create_ptt_news_upload(req: PTTNewsUploadRequest):
    """建立 PTT 新聞上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "ptt_news",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_ptt_news_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/ptt-news/uploaded")
def list_uploaded_ptt_news():
    """列出已上傳的 PTT 新聞日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM PTTUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 PTT 新聞日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/ptt-news/schedule")
def get_ptt_news_schedule():
    """取得 PTT 新聞每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    ptt = config.get("ptt_schedule", {"time": "21:20"})
    return {"time": ptt["time"]}


@app.put("/api/ptt-news/schedule")
def update_ptt_news_schedule(req: PTTNewsScheduleRequest):
    """更新 PTT 新聞每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["ptt_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config["ptt_schedule"],
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("PTT 新聞每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"PTT 新聞每日排程已更新為 {req.time}",
    }


# MoneyUDN 新聞 API 端點
@app.post("/api/moneyudn-news/upload")
def create_moneyudn_news_upload(req: MoneyUDNNewsUploadRequest):
    """建立 MoneyUDN 新聞上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "moneyudn_news",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_moneyudn_news_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/moneyudn-news/uploaded")
def list_uploaded_moneyudn_news():
    """列出已上傳的 MoneyUDN 新聞日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM MoneyUDNUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 MoneyUDN 新聞日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/moneyudn-news/schedule")
def get_moneyudn_news_schedule():
    """取得 MoneyUDN 新聞每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    moneyudn = config.get("moneyudn_schedule", {"time": "21:22"})
    return {"time": moneyudn["time"]}


@app.put("/api/moneyudn-news/schedule")
def update_moneyudn_news_schedule(req: MoneyUDNNewsScheduleRequest):
    """更新 MoneyUDN 新聞每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["moneyudn_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config["moneyudn_schedule"],
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("MoneyUDN 新聞每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"MoneyUDN 新聞每日排程已更新為 {req.time}",
    }


# YT 逐字稿 API 端點
@app.post("/api/yt-transcript/upload")
def create_yt_transcript_upload(req: YTTranscriptUploadRequest):
    """建立 YT 逐字稿上傳任務。

    Args:
        req: 包含日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    if not _validate_date_format(req.date):
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "yt_transcript",
            "status": "queued",
            "date": req.date,
            "title": None,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_yt_transcript_upload_job, (job_id, req.date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/yt-transcript/uploaded")
def list_uploaded_yt_transcript():
    """列出已成功的 YT 逐字稿日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM YTTranscript "
                    "WHERE Status = 'success' "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()
        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}
    except Exception as e:
        logger.error("查詢已上傳 YT 逐字稿日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/yt-transcript/schedule")
def get_yt_transcript_schedule():
    """取得 YT 逐字稿每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    yt = config.get("yt_transcript_schedule", {"time": "21:24"})
    return {"time": yt["time"]}


@app.put("/api/yt-transcript/schedule")
def update_yt_transcript_schedule(req: YTTranscriptScheduleRequest):
    """更新 YT 逐字稿每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["yt_transcript_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config["yt_transcript_schedule"],
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("YT 逐字稿每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"YT 逐字稿每日排程已更新為 {req.time}",
    }


@app.get("/api/yt-transcript/status")
def get_yt_transcript_status(
    date: str = Query(description="日期，格式 YYYY-MM-DD"),
):
    """查詢指定日期的 YT 逐字稿抓取狀態。

    Args:
        date: 日期字串（YYYY-MM-DD）。

    Returns:
        dict: 包含逐字稿狀態資訊。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "NEWS") as conn:
            row = conn.execute(
                text(
                    "SELECT Date, Title, url, Duration, ContentFile, "
                    "Status, ErrorMessage FROM YTTranscript WHERE Date = :date"
                ),
                {"date": date},
            ).fetchone()

        if not row:
            return {"exists": False}

        return {
            "exists": True,
            "date": str(row[0]),
            "title": row[1],
            "url": row[2],
            "duration": row[3],
            "content_file": row[4],
            "status": row[5],
            "error_message": row[6],
        }
    except Exception as e:
        logger.error("查詢 YT 逐字稿狀態失敗: %s", e)
        return {"exists": False}


# 原油價格 API 端點
@app.post("/api/oil-price/upload")
def create_oil_price_upload(req: OilPriceUploadRequest):
    """建立原油價格上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "oil_price",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_oil_price_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/oil-price/uploaded")
def list_uploaded_oil_price():
    """列出已上傳的原油價格日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM OilPriceUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳原油價格日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/oil-price/schedule")
def get_oil_price_schedule():
    """取得原油價格每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    oil = config.get("oil_price_schedule", {"time": "21:06"})
    return {"time": oil["time"]}


@app.put("/api/oil-price/schedule")
def update_oil_price_schedule(req: OilPriceScheduleRequest):
    """更新原油價格每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["oil_price_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config["oil_price_schedule"],
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("原油價格每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"原油價格每日排程已更新為 {req.time}",
    }


# 黃金價格 API 端點
@app.post("/api/gold-price/upload")
def create_gold_price_upload(req: GoldPriceUploadRequest):
    """建立黃金價格上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "gold_price",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_gold_price_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/gold-price/uploaded")
def list_uploaded_gold_price():
    """列出已上傳的黃金價格日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM GoldPriceUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳黃金價格日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/gold-price/schedule")
def get_gold_price_schedule():
    """取得黃金價格每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    gold = config.get("gold_price_schedule", {"time": "21:08"})
    return {"time": gold["time"]}


@app.put("/api/gold-price/schedule")
def update_gold_price_schedule(req: GoldPriceScheduleRequest):
    """更新黃金價格每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["gold_price_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config["gold_price_schedule"],
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("黃金價格每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"黃金價格每日排程已更新為 {req.time}",
    }


# 比特幣價格 API 端點
@app.post("/api/bitcoin-price/upload")
def create_bitcoin_price_upload(req: BitcoinPriceUploadRequest):
    """建立比特幣價格上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "bitcoin_price",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_bitcoin_price_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/bitcoin-price/uploaded")
def list_uploaded_bitcoin_price():
    """列出已上傳的比特幣價格日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM BitcoinPriceUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳比特幣價格日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/bitcoin-price/schedule")
def get_bitcoin_price_schedule():
    """取得比特幣價格每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    bitcoin = config.get("bitcoin_price_schedule", {"time": "21:10"})
    return {"time": bitcoin["time"]}


@app.put("/api/bitcoin-price/schedule")
def update_bitcoin_price_schedule(req: BitcoinPriceScheduleRequest):
    """更新比特幣價格每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["bitcoin_price_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config["bitcoin_price_schedule"],
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("比特幣價格每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"比特幣價格每日排程已更新為 {req.time}",
    }


# 匯率 API 端點
@app.post("/api/currency-price/upload")
def create_currency_price_upload(req: CurrencyPriceUploadRequest):
    """建立匯率上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "currency_price",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_currency_price_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/currency-price/uploaded")
def list_uploaded_currency_price():
    """列出已上傳的匯率日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM CurrencyPriceUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳匯率日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/currency-price/schedule")
def get_currency_price_schedule():
    """取得匯率每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    currency = config.get("currency_price_schedule", {"time": "21:12"})
    return {"time": currency["time"]}


@app.put("/api/currency-price/schedule")
def update_currency_price_schedule(req: CurrencyPriceScheduleRequest):
    """更新匯率每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["currency_price_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config["currency_price_schedule"],
        config.get("indices_price_schedule"),
        config.get("special_info_backfill_schedule"),
    )

    logger.info("匯率每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"匯率每日排程已更新為 {req.time}",
    }


# 股市指數價格 API 端點
@app.post("/api/indices-price/upload")
def create_indices_price_upload(req: IndicesPriceUploadRequest):
    """建立股市指數價格上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "indices_price",
            "status": "queued",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_indices_price_upload_job,
        (job_id, req.start_date, req.end_date),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/indices-price/uploaded")
def list_uploaded_indices_price():
    """列出已上傳的股市指數價格日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "SPECIAL_INFO") as conn:
            rows = conn.execute(
                text(
                    "SELECT Date FROM IndicesPriceUploaded "
                    "ORDER BY Date DESC LIMIT 50"
                )
            ).fetchall()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳股市指數價格日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/indices-price/schedule")
def get_indices_price_schedule():
    """取得股市指數價格每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    indices = config.get("indices_price_schedule", {"time": "21:14"})
    return {"time": indices["time"]}


@app.put("/api/indices-price/schedule")
def update_indices_price_schedule(req: IndicesPriceScheduleRequest):
    """更新股市指數價格每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["indices_price_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config["indices_price_schedule"],
        config.get("special_info_backfill_schedule"),
    )

    logger.info("股市指數價格每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"股市指數價格每日排程已更新為 {req.time}",
    }


# SPECIAL_INFO 缺漏自我修復 API 端點
@app.get("/api/special-info-backfill/schedule")
def get_special_info_backfill_schedule():
    """取得 SPECIAL_INFO 缺漏自我修復每日排程設定。

    Returns:
        dict: 包含 time 與 days 欄位的排程資訊。
    """
    config = load_config()
    backfill = config.get(
        "special_info_backfill_schedule", {"time": "21:27"}
    )
    return {"time": backfill["time"], "days": SPECIAL_INFO_BACKFILL_DAYS}


@app.put("/api/special-info-backfill/schedule")
def update_special_info_backfill_schedule(
    req: SpecialInfoBackfillScheduleRequest,
):
    """更新 SPECIAL_INFO 缺漏自我修復每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
    """
    # 與 load_config 的形狀正規化共用同一判準：端點放行、重啟卻被判為格式不符而
    # 換回預設值的話，使用者的設定等於被靜默丟棄（例如 "07:30:00"、"7:30"）。
    if not _is_valid_time(req.time):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["special_info_backfill_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
        config.get("yt_transcript_schedule"),
        config.get("oil_price_schedule"),
        config.get("gold_price_schedule"),
        config.get("bitcoin_price_schedule"),
        config.get("currency_price_schedule"),
        config.get("indices_price_schedule"),
        config["special_info_backfill_schedule"],
    )

    logger.info("SPECIAL_INFO 缺漏自我修復每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"SPECIAL_INFO 缺漏自我修復每日排程已更新為 {req.time}",
    }


@app.post("/api/special-info-backfill/run")
def create_special_info_backfill_run(req: SpecialInfoBackfillRunRequest = None):
    """手動觸發 SPECIAL_INFO 缺漏自我修復偵測補抓任務。

    Args:
        req: 可選，包含 days（掃描天數）與 deep（是否深度重驗）。未提供時
            使用預設 30 天、deep=False；deep=False 仍會重驗最近
            SPECIAL_INFO_REVERIFY_DAYS 天的孤兒帳本。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    days = req.days if req is not None else SPECIAL_INFO_BACKFILL_DAYS
    deep = req.deep if req is not None else False
    if days <= 0:
        raise HTTPException(400, "days 必須為正整數")

    job_id = str(uuid.uuid4())[:8]
    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "special_info_backfill",
            "status": "queued",
            "days": days,
            "deep": deep,
            "record_count": 0,
            "summary": [],
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    # deep=False 時仍帶入重驗天數（與 21:27 排程一致）：否則手動觸發非深度
    # 補抓等於完全不清孤兒帳本，操作者照文件來救誤標會一無所獲。
    position = job_queue.enqueue(
        job_id, run_special_info_backfill_job,
        (job_id, days, deep, None, SPECIAL_INFO_REVERIFY_DAYS),
    )
    logger.info(
        "SPECIAL_INFO 缺漏自我修復手動任務已建立 %s（近 %d 天，deep=%s，"
        "重驗近 %d 天）",
        job_id, days, deep, SPECIAL_INFO_REVERIFY_DAYS,
    )
    return {"job_id": job_id, "status": "queued", "queue_position": position}


# 公司產業對照 API 端點
@app.post("/api/company-info/upload")
def create_company_info_upload():
    """建立公司產業對照上傳任務。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "company_info",
            "status": "queued",
            "company_info_count": 0,
            "industry_map_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    position = job_queue.enqueue(
        job_id, run_company_info_upload_job, (job_id,),
    )

    return {"job_id": job_id, "status": "queued", "queue_position": position}


@app.get("/api/company-info/status")
def get_company_info_status():
    """取得 CompanyInfo 和 IndustryMap 表的資料筆數。

    Returns:
        dict: 包含 company_info_count 和 industry_map_count。
    """
    try:
        with db_conn(HOST, USER, PASSWORD, "TWSE") as conn:
            company_count = conn.execute(
                text("SELECT COUNT(*) FROM CompanyInfo")
            ).scalar()
            industry_count = conn.execute(
                text("SELECT COUNT(*) FROM IndustryMap")
            ).scalar()

        return {
            "company_info_count": company_count,
            "industry_map_count": industry_count,
        }

    except Exception as e:
        logger.error("查詢公司產業對照狀態失敗：%s", e)
        return {
            "company_info_count": 0,
            "industry_map_count": 0,
        }


# 重試佇列 API 端點
@app.get("/api/retry-queue")
def get_retry_queue():
    """取得所有重試任務與網路狀態。

    Returns:
        dict: 包含 tasks、network_available 和 summary。
    """
    tasks = retry_queue.get_all()
    network_ok = check_network_available(CRAWLERHOST)
    return {
        "tasks": [asdict(t) for t in tasks],
        "network_available": network_ok,
        "summary": {
            "pending": sum(1 for t in tasks if t.status == "pending"),
            "retrying": sum(1 for t in tasks if t.status == "retrying"),
            "success": sum(1 for t in tasks if t.status == "success"),
            "exhausted": sum(1 for t in tasks if t.status == "exhausted"),
        },
    }


@app.post("/api/retry-queue/retry-all")
def retry_all_pending():
    """手動立即觸發重試所有 pending 任務。

    與每小時排程共用同一支包裝函式與重入旗標：兩者若同時執行，同一筆任務會被
    重複跑一遍（新聞雖以 URL 去重，行情類 `DailyPrice` 卻是 append 寫入，
    會產生重複列）。

    Returns:
        dict: 操作結果訊息與 `started`（是否真的啟動了一輪重試）。前端據
            `started` 區分成功與略過的樣式，避免「略過」被顯示成綠色成功。
    """
    if run_retry_queue_scheduled():
        return {"message": "已觸發重試所有 pending 任務", "started": True}
    return {"message": "上一輪重試尚未結束，本次略過", "started": False}


@app.post("/api/retry-queue/reset-exhausted")
def reset_exhausted_tasks():
    """將所有 exhausted 任務重設為 pending。

    Returns:
        dict: 操作結果訊息與重設數量。
    """
    count = retry_queue.reset_exhausted()
    return {
        "message": f"已重設 {count} 筆 exhausted 任務",
        "reset_count": count,
    }


@app.delete("/api/retry-queue/clear")
def clear_completed_retry_tasks():
    """清除所有已完成的重試任務。

    Returns:
        dict: 操作結果訊息與清除數量。
    """
    count = retry_queue.clear_completed()
    return {
        "message": f"已清除 {count} 筆已完成任務",
        "cleared_count": count,
    }


@app.delete("/api/retry-queue/clear-exhausted")
def clear_exhausted_retry_tasks():
    """清除所有已放棄（exhausted）的重試任務。

    Returns:
        dict: 操作結果訊息與清除數量。
    """
    count = retry_queue.clear_exhausted()
    return {
        "message": f"已清除 {count} 筆 exhausted 任務",
        "cleared_count": count,
    }


@app.post("/api/retry-queue/requeue-exhausted")
def requeue_exhausted_retry_tasks():
    """手動觸發隔日重排：將未達上限的 exhausted 任務重設為 pending。

    Returns:
        dict: 操作結果訊息與重排、維持數量。
    """
    requeued, kept = retry_queue.requeue_exhausted()
    return {
        "message": f"已重排 {requeued} 筆，{kept} 筆已達上限維持 exhausted",
        "requeued_count": requeued,
        "kept_count": kept,
    }


@app.delete("/api/retry-queue/{task_id}")
def remove_retry_task(task_id: str):
    """移除單一重試任務。

    Args:
        task_id: 任務 ID。

    Returns:
        dict: 操作結果訊息。

    Raises:
        HTTPException: 任務不存在時拋出 404。
    """
    if not retry_queue.remove(task_id):
        raise HTTPException(404, "任務不存在")
    return {"message": "任務已移除"}


# Serve React 前端靜態檔案
@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    """Serve React 前端頁面與靜態資源。

    Args:
        full_path: 請求路徑。

    Returns:
        FileResponse: 靜態檔案或 index.html（SPA fallback）。
    """
    if not STATIC_DIR.exists():
        raise HTTPException(404, "前端頁面尚未建構")

    # 防止路徑穿越攻擊
    if full_path:
        file_path = (STATIC_DIR / full_path).resolve()
        if not str(file_path).startswith(str(STATIC_DIR.resolve())):
            raise HTTPException(403, "禁止存取")
        if file_path.is_file():
            return FileResponse(file_path)

    # SPA fallback：回傳 index.html
    index_file = STATIC_DIR / "index.html"
    if index_file.is_file():
        return FileResponse(index_file)

    raise HTTPException(404, "頁面不存在")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

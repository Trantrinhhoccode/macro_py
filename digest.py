"""
TApro Daily Digest — chạy độc lập, không cần DB.
Crawl tin → Gemini score → Tạo bản tin → Gửi Telegram.
"""
import difflib
import gzip
import json
import os
import re
import smtplib
import threading
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

VN_TZ = timezone(timedelta(hours=7))   # UTC+7, dùng thống nhất thay cho now_vn()


def now_vn() -> datetime:
    """Trả về datetime hiện tại theo giờ Việt Nam (UTC+7), chạy đúng cả local lẫn GitHub Actions."""
    return datetime.now(VN_TZ)
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html.parser import HTMLParser

import requests

# ─── Global Gemini rate limiter (15 RPM = 1 req / 4s) ────────────────────────
_GEMINI_LOCK      = threading.Lock()
_GEMINI_LAST_CALL = 0.0
_GEMINI_MIN_GAP   = 4.1   # giây giữa 2 lần gọi API (4.1s ≈ 14.6 RPM, safe dưới 15)

def _gemini_wait():
    """Chờ đủ khoảng cách tối thiểu giữa các lần gọi Gemini (global, thread-safe)."""
    global _GEMINI_LAST_CALL
    with _GEMINI_LOCK:
        gap = time.time() - _GEMINI_LAST_CALL
        if gap < _GEMINI_MIN_GAP:
            time.sleep(_GEMINI_MIN_GAP - gap)
        _GEMINI_LAST_CALL = time.time()

# ─── Config (đọc từ env / GitHub Secrets) ────────────────────────────────────
GEMINI_KEY   = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
EMAIL_SENDER   = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_APP_PASSWORD", "")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "")
BOT_TOKEN    = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID      = os.environ["TELEGRAM_CHAT_ID"]
MIN_SCORE    = int(os.getenv("MIN_SCORE", "7"))

HDR = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

# ─── HTML fetch + parse ───────────────────────────────────────────────────────
def fetch(url: str) -> str | None:
    try:
        req = urllib.request.Request(url, headers=HDR)
        with urllib.request.urlopen(req, timeout=15) as r:
            data = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                data = gzip.decompress(data)
            return data.decode("utf-8", errors="ignore")
    except Exception:
        return None


_MAX_AGE_HOURS = 72   # chỉ lấy bài đăng trong vòng 72h (3 ngày) gần nhất


def _url_date_too_old(url: str) -> bool:
    """Pre-filter nhanh: nếu URL chứa YYYYMMDD và quá _MAX_AGE_HOURS → bỏ (không cần fetch)."""
    m = re.search(r'(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])', url)
    if not m:
        return False   # URL không rõ ngày → không lọc
    try:
        pub = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                       tzinfo=timezone.utc)
        age_h = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
        return age_h > _MAX_AGE_HOURS
    except Exception:
        return False


class _Parser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_h1 = self.in_p = self.in_title = False
        self.title = ""
        self.content: list[str] = []
        self.description = ""
        self.pub_date = ""     # article:published_time nếu có

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        cls = d.get("class", "").lower()
        if tag == "h1":
            self.in_h1 = True
        elif tag == "p" and any(k in cls for k in ("description", "sapo", "lead", "intro")):
            self.in_p = True
        elif tag == "p":
            self.in_p = True
        elif tag == "meta":
            prop = d.get("property", "") or d.get("name", "")
            if prop in ("og:description", "description") and not self.description:
                self.description = d.get("content", "")
            # Lấy ngày đăng từ OpenGraph / standard meta
            if prop in ("article:published_time", "og:article:published_time",
                        "article:modified_time", "og:updated_time") and not self.pub_date:
                self.pub_date = d.get("content", "")
        elif tag == "title":
            self.in_title = True

    def handle_endtag(self, tag):
        if tag == "h1":  self.in_h1 = False
        if tag == "p":   self.in_p = False
        if tag == "title": self.in_title = False

    def handle_data(self, data):
        d = data.strip()
        if not d: return
        if self.in_h1 and not self.title:    self.title = d
        elif self.in_title and not self.title: self.title = d
        elif self.in_p: self.content.append(d)


def parse(url: str, html: str) -> dict:
    p = _Parser()
    try: p.feed(html)
    except Exception: pass
    body = " ".join(p.content)

    # Fallback: thử đọc datePublished từ JSON-LD nếu meta tag không có
    pub_date = p.pub_date
    if not pub_date:
        ld = re.search(r'"datePublished"\s*:\s*"([^"]+)"', html[:15000])
        if ld:
            pub_date = ld.group(1)

    return {
        "url": url,
        "title": p.title or "",
        "description": p.description or "",
        "word_count": len(body.split()),
        "content_first_500": body[:5000],
        "pub_date": pub_date,
        "has_numbers": bool(re.search(
            r"\d+([.,]\d+)?\s*(%|tỷ|triệu|nghìn|USD|VNĐ|VND|đồng|điểm|tấn|km)", body, re.I
        )),
    }


# ─── Source collectors ────────────────────────────────────────────────────────
def collect_vneconomy() -> list[str]:
    html = fetch("https://vneconomy.vn/")
    if not html: return []
    raw: set[str] = set()
    for u in re.findall(r'href="([^"]*\.htm)"', html):
        if u.startswith("https://vneconomy.vn"): raw.add(u[len("https://vneconomy.vn"):])
        elif u.startswith("/"): raw.add(u)
    SKIP_PRE = ["/automotive/","/event/","/an-pham","/cafe-bds","/dang-cong-san",
                "/bau-cu","/diem-tin-","/photo","/video","/podcast","/emagazine"]
    SKIP_KW  = ["du-lich","am-thuc","giai-tri","the-thao","sac-dep","thoi-trang",
                "oto-xe-may","lifestyle","song-khoe","nha-dep","kham-chua-benh"]
    SKIP_SUF = re.compile(r"-(e|s)\d+\.htm$")
    out = []
    for u in raw:
        if any(u.startswith(p) for p in SKIP_PRE): continue
        if any(kw in u for kw in SKIP_KW): continue
        if SKIP_SUF.search(u): continue
        # Bài báo VnEconomy có dạng /category/slug.htm (≥2 cấp path)
        # URL 1 cấp (/tag-name.htm) là trang category/tag → loại
        if u.count("/") < 2: continue
        if len(u.rstrip("/").replace(".htm","").split("/")[-1].split("-")) < 5: continue
        out.append("https://vneconomy.vn" + u)
    return list(set(out))


def collect_cafef() -> list[str]:
    html = fetch("https://cafef.vn/")
    if not html: return []
    out: set[str] = set()
    for u in re.findall(r'href="(/[^"]*-\d{8,}\.chn)"', html):
        out.add("https://cafef.vn" + u)
    SKIP = ["xon-xao","la-lung","ky-la","gay-soc","soc-","-nho-duoc","khien","phat-hien","tu-vong"]
    return [u for u in out if not any(k in u for k in SKIP)]


def collect_sgt() -> list[str]:
    html = fetch("https://thesaigontimes.vn/")
    if not html: return []
    out: set[str] = set()
    for u in re.findall(r'href="(https://thesaigontimes\.vn/[a-z0-9\-]+/)"', html):
        if "/category/" in u: continue
        if len(u.rstrip("/").split("/")[-1].split("-")) < 5: continue
        out.add(u)
    return list(out)


def collect_ncdt() -> list[str]:
    html = fetch("https://nhipcaudautu.vn/")
    if not html: return []
    out: set[str] = set()
    # URL bài mới có dạng: /<category>/<slug>-<id>/  (id 6+ chữ số)
    for u in re.findall(r'href="(/[a-z][a-z0-9\-]+/[a-z0-9\-]+-\d{6,}/?)"', html):
        if not any(s in u for s in ["/tag/", "/category/", "/author/"]):
            out.add("https://nhipcaudautu.vn" + u)
    # Pattern cũ phòng khi vẫn còn .html
    for u in re.findall(r'href="(https://nhipcaudautu\.vn/[^"]+\.html?)"', html):
        if not any(s in u for s in ["/tag/", "/category/", "/author/"]):
            out.add(u)
    return list(out)


def collect_bdt() -> list[str]:
    html = fetch("https://baodautu.vn/")
    if not html: return []
    out: set[str] = set()
    for u in re.findall(r'href="(https://baodautu\.vn/[^"]+\.html?)"', html):
        if not any(s in u for s in ["/tag/","/category/","/page/"]):
            out.add(u)
    for u in re.findall(r'href="(/[a-z][^"]*\.html?)"', html):
        if not any(s in u for s in ["/tag/","/category/","/page/"]):
            out.add("https://baodautu.vn" + u)
    return list(out)


def collect_bbw() -> list[str]:
    html = fetch("https://bbw.vn/")
    if not html: return []
    out: set[str] = set()
    for u in re.findall(r'href="(https://bbw\.vn/[a-z0-9][a-z0-9\-]+-\d+\.html)"', html):
        out.add(u)
    for u in re.findall(r'href="(/[a-z0-9][a-z0-9\-]+-\d+\.html)"', html):
        out.add("https://bbw.vn" + u)
    return list(out)


# ─── Tier 2: fetch + filter ────────────────────────────────────────────────────
def _parse_pub_age_hours(pub_date: str) -> float | None:
    """Tính số giờ kể từ khi bài đăng. Trả None nếu không parse được."""
    if not pub_date:
        return None
    try:
        dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600
    except Exception:
        return None


def process(src: str, url: str) -> dict | None:
    # Lớp 1: URL pre-filter — không cần fetch nếu URL chứa YYYYMMDD quá cũ
    if _url_date_too_old(url):
        return None

    html = fetch(url)
    if not html: return None
    if src == "BBW":
        title_m = re.search(r'<title[^>]*>([^<]+)</title>', html[:5000])
        idx = html.find('id="article-detail"')
        if idx > 0:
            html = html[idx:]
            if title_m:
                html = f'<h1>{title_m.group(1)}</h1>' + html
    art = parse(url, html)
    art["source"] = src
    if art["word_count"] < 300: return None
    if not art["has_numbers"]: return None
    if not art["title"]: return None
    if re.search(r"(\?|sốc|kinh hoàng|bất ngờ|gây chú ý)", art["title"], re.I): return None
    # Loại trang category/tag (title kết thúc bằng "- VnEconomy", "| CafeF"...)
    if re.search(r"\s[-|]\s*(VnEconomy|CafeF|Báo Đầu Tư|Nhịp Cầu Đầu Tư|The Saigon Times)\s*$",
                 art["title"], re.I):
        return None

    # Lớp 2: Meta date filter — dùng article:published_time nếu có
    age_h = _parse_pub_age_hours(art.get("pub_date", ""))
    if age_h is not None and age_h > _MAX_AGE_HOURS:
        return None   # bài cũ hơn 72h → bỏ

    return art


# ─── Tier 3: Gemini score ─────────────────────────────────────────────────────
SCORE_PROMPT = """Chấm điểm bài báo cho nhà đầu tư chứng khoán Việt Nam.
TIÊU ĐỀ: {title}
NỘI DUNG: {content}

Chấm 0-3 mỗi tiêu chí:
- depth: phân tích sâu(3)|tổng hợp(2)|tin nhanh(1)|PR(0)
- impact: vĩ mô lớn(3)|ngành(2)|doanh nghiệp lớn(1)|nhỏ(0)
- novelty: insight mới(3)|góc nhìn mới(2)|số liệu(1)|re-up(0)
- actionable: thúc đẩy quyết định đầu tư(3)|có ích(2)|tham khảo(1)|giải trí(0)

CHỈ trả về JSON:
{{"depth":N,"impact":N,"novelty":N,"actionable":N,"topic":"Vĩ mô VN|Vĩ mô TG|Chính sách|Ngành|Doanh nghiệp|Thị trường|Khác","mood":"tích cực|cảnh báo|trung lập","summary":"1 câu tóm tắt tiếng Việt"}}"""


def score(art: dict) -> dict | None:
    prompt = SCORE_PROMPT.format(title=art["title"], content=art["content_first_500"])
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}],
               "generationConfig": {"temperature": 0.1, "maxOutputTokens": 300}}
    for attempt in range(3):
        try:
            _gemini_wait()   # global rate limiter: tối đa 1 req/4.1s
            r = requests.post(url, json=payload, params={"key": GEMINI_KEY}, timeout=20)
            if r.status_code == 429:
                time.sleep(20 * (attempt + 1))   # 20s, 40s, 60s
                continue
            r.raise_for_status()
            text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
            text = re.sub(r"```(?:json)?", "", text).replace("```", "").strip()
            m = re.search(r"\{.*\}", text, re.DOTALL)
            if m:
                s = json.loads(m.group())
                s["total"] = sum(s.get(k, 0) for k in ("depth", "impact", "novelty", "actionable"))
                return s
        except Exception:
            time.sleep(2)
    return None


# ─── Build digest ─────────────────────────────────────────────────────────────
def fetch_full_text(url: str) -> str:
    html = fetch(url)
    if not html: return ""
    if "bbw.vn" in url:
        idx = html.find('id="article-detail"')
        if idx > 0: html = html[idx:]
    paras = re.findall(r"<p[^>]*>([^<]{40,})</p>", html)
    text = re.sub(r"\s+", " ", " ".join(paras)).strip()
    return text[:1500]


def build_digest(articles: list[dict]) -> str:
    # Lưu map index → url để thay thế sau (Gemini KHÔNG tự chọn URL nữa)
    url_map = {i: a["url"] for i, a in enumerate(articles, 1)}

    articles_text = ""
    for i, a in enumerate(articles, 1):
        content = fetch_full_text(a["url"])
        articles_text += (
            f"--- BÀI {i} ---\n"
            f"Tiêu đề: {a['title']}\n"
            f"Nguồn: {a['source']} | Chủ đề: {a['score']['topic']} | Mood: {a['score']['mood']}\n"
            f"Nội dung: {content}\n\n"
        )
    today = now_vn().strftime("%d/%m/%Y")
    prompt = f"""Bạn là chuyên gia phân tích đầu tư chứng khoán Việt Nam.
Dưới đây là nội dung {len(articles)} bài báo chất lượng cao ngày {today}:

{articles_text}

Viết BẢN TIN chi tiết theo CHÍNH XÁC định dạng dưới đây.

QUY TẮC ĐỊNH DẠNG (BẮT BUỘC TUÂN THỦ):
1. CHỈ dùng **dấu sao đôi** để in đậm (KHÔNG dùng dấu sao đơn).
2. KHÔNG dùng dấu * làm bullet. Dùng dấu gạch ngang "- " hoặc số thứ tự "1. ".
3. In đậm BẰNG ** TẤT CẢ con số/dữ liệu định lượng:
   - Phần trăm: **38,2%**, **+15,7%**, **-2,3%**, **YoY +12%**
   - Số tiền/quy mô: **2.500 tỷ đồng**, **150 triệu USD**
   - Lãi suất: **8,38%/năm**
   - Chỉ số: **VN-Index 1.250 điểm**, **VN30**, **NIM 3,2%**, **ROE 18%**
   - Mọi số liệu khác có ý nghĩa thị trường
4. Tiêu đề mỗi tin: dùng **...** in đậm.
5. Sau MỖI tin (dưới đoạn nội dung), thêm DÒNG RIÊNG chính xác:
   {{{{LINK_N}}}}  ← thay N bằng số thứ tự bài (1, 2, 3...). Ví dụ bài 1 → {{{{LINK_1}}}}, bài 2 → {{{{LINK_2}}}}
   TUYỆT ĐỐI không tự điền URL, chỉ đặt đúng marker {{{{LINK_N}}}}.
6. Mỗi tin 4-6 câu, trích số liệu/tên chuyên gia/dữ kiện cụ thể.
7. Không bỏ sót bài nào.

CẤU TRÚC OUTPUT:

📊 **BẢN TIN THỊ TRƯỜNG — {today}**

🇻🇳 **KINH TẾ VIỆT NAM**

**[Tiêu đề bài 1]**
[4-6 câu nội dung, BOLD mọi số liệu]
{{{{LINK_1}}}}

**[Tiêu đề bài 2]**
[...]
{{{{LINK_2}}}}

🌍 **VĨ MÔ THẾ GIỚI**

[Format tương tự nếu có bài quốc tế]

⚠️ **RỦI RO CẦN THEO DÕI**

1. **[Tiêu đề rủi ro 1]:** [Mô tả có số liệu BOLD]
2. **[Tiêu đề rủi ro 2]:** [Mô tả có số liệu BOLD]
3. **[Tiêu đề rủi ro 3]:** [Mô tả có số liệu BOLD]

💡 **GỢI Ý NGÀNH/CỔ PHIẾU**

- **[Tên ngành/cổ phiếu]:** [Lý do cụ thể từ tin trên, có số liệu BOLD]
- **[Tên ngành/cổ phiếu]:** [...]
- **[Tên ngành/cổ phiếu]:** [...]"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}],
               "generationConfig": {"temperature": 0.3, "maxOutputTokens": 4000}}

    # Dùng global rate limiter — đảm bảo không gọi quá 15 RPM
    for attempt in range(3):
        _gemini_wait()
        r = requests.post(url, json=payload, params={"key": GEMINI_KEY}, timeout=90)
        if r.status_code == 429:
            wait = 30 * (attempt + 1)
            print(f"  build_digest 429 — chờ {wait}s (lần {attempt+1}/3)...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        break
    else:
        raise RuntimeError("build_digest: vẫn bị 429 sau 3 lần thử")

    text = r.json()["candidates"][0]["content"]["parts"][0]["text"]

    # Thay thế marker {{LINK_N}} bằng URL đúng của bài N
    # (Gemini chỉ đặt marker, không tự chọn URL → tránh gắn nhầm link)
    for i, article_url in url_map.items():
        text = text.replace(f"{{{{LINK_{i}}}}}", f"[đọc thêm]({article_url})")
    # Dọn marker còn sót (phòng khi Gemini bỏ sót)
    text = re.sub(r"\{\{LINK_\d+\}\}", "", text)

    return text


# ─── Polymarket ───────────────────────────────────────────────────────────────
# Nhóm anchor: (emoji, tên nhóm VN, danh sách keyword tìm trong question tiếng Anh)
# Mỗi nhóm: keyword phải có MẶT, không_có danh sách keyword bị loại trừ
_POLY_ANCHOR_GROUPS = [
    # (emoji, display, must_have_any, must_not_have_any)
    ("🏦", "Fed / Lãi suất",
        ["rate cut", "rate cuts", "no fed rate", "fed rate cut"],
        ["increase interest", "raise interest"]),
    ("📉", "Suy thoái Mỹ",
        ["recession"],
        []),
    ("🛢",  "Iran & Hormuz",
        ["hormuz", "iran peace", "iran deal", "us x iran"],
        []),
    ("🇹🇼", "Trung Quốc / Đài Loan",
        ["china invade taiwan", "china blockade taiwan", "china x taiwan",
         "china taiwan", "taiwan invasion"],
        []),
]

# Loại bỏ các market không liên quan (thể thao, giải trí, chính trị xa)
_POLY_EXCLUDE_KW = [
    "2028", "fifa", "world cup", "nba", "nfl", "ufc", "mlb", "nhl",
    "album", "jesus", "alien", "kardashian", "lebron", "rihanna", "carti",
    "mrbeast", "clooney", "sanders", "warnock", "pence", "thune", "beto",
    "cheney", "clinton", "walz", "obama", "trump jr", "eric trump",
    "baseball", "basketball", "hockey", "soccer", "tennis", "golf",
    "spread:", "o/u ", " vs. ",  # sports lines
    "tweet", "post ", "elon musk post",
]


def _poly_bar(prob: float, width: int = 10) -> str:
    filled = round(prob * width)
    return "█" * filled + "░" * (width - filled)


def _poly_parse_market(m: dict) -> dict | None:
    """Trích xuất YES%, NO%, vol từ 1 market object. Trả None nếu không parse được."""
    try:
        outcomes = m.get("outcomes", "[]")
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        prices = m.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        if len(outcomes) != 2 or len(prices) != 2:
            return None
        yes_prob = float(prices[0])
        return {
            "question": m.get("question", ""),
            "yes":   round(yes_prob * 100),
            "no":    round((1 - yes_prob) * 100),
            "vol":   m.get("volumeNum",  0) or 0,
            "vol24": m.get("volume24hr", 0) or 0,
        }
    except Exception:
        return None


def _poly_is_junk(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in _POLY_EXCLUDE_KW)


def fetch_polymarket() -> dict:
    """Fetch Polymarket: anchor markets + top-5 hot. Fetch 1 lần, filter client-side."""
    result: dict = {"anchors": [], "hot": []}
    base = "https://gamma-api.polymarket.com/markets"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        # ── Fetch 500 market theo tổng volume (1 request duy nhất) ───────────
        r = requests.get(
            base,
            params={"limit": 500, "order": "volumeNum", "ascending": "false",
                    "active": "true", "closed": "false"},
            headers=headers, timeout=30,
        )
        if r.status_code != 200:
            return result
        all_markets = r.json()

        # Parse tất cả thành dicts, lọc binary + junk
        parsed_all: list[dict] = []
        for m in all_markets:
            if _poly_is_junk(m.get("question", "")):
                continue
            p = _poly_parse_market(m)
            if p:
                parsed_all.append(p)

        # ── Anchor: với mỗi nhóm, lấy market khớp keyword, vol cao nhất, còn open ─
        for emoji, display, must_have, must_not in _POLY_ANCHOR_GROUPS:
            best = None
            for p in parsed_all:
                q = p["question"].lower()
                # Phải có ít nhất 1 keyword
                if not any(kw in q for kw in must_have):
                    continue
                # Không được chứa keyword loại trừ
                if any(kw in q for kw in must_not):
                    continue
                # Bỏ qua market đã resolve (YES=0% hoặc 100%)
                if p["yes"] in (0, 100):
                    continue
                best = p
                break   # parsed_all đã sort theo volumeNum giảm dần
            if best:
                result["anchors"].append({**best, "display": display, "emoji": emoji})

        # ── Hot: top-5 theo volume 24h (đã loại junk) ────────────────────────
        sorted_by_24h = sorted(parsed_all, key=lambda x: -x["vol24"])
        result["hot"] = sorted_by_24h[:5]
    except Exception as e:
        print(f"  ⚠️  Polymarket fetch lỗi: {e}")
    return result


_POLY_CHANGE_THRESHOLD = 3   # hiển thị anchor nếu YES% thay đổi >= 3% so với lần trước


def build_polymarket_section(data: dict, prev_anchors: dict) -> tuple[str, dict]:
    """Tạo chuỗi hiển thị Polymarket + dict anchor mới để lưu vào sent.

    prev_anchors: {display_name: yes_pct} đọc từ sent_urls.json
    Trả về: (section_string, updated_prev_anchors)
    """
    # ── Lọc anchor: chỉ show nếu thay đổi >= threshold hoặc chưa từng ghi nhận ──
    changed: list[dict] = []
    new_prev = dict(prev_anchors)   # copy để cập nhật
    for a in data.get("anchors", []):
        key  = a["display"]
        prev = prev_anchors.get(key)  # None nếu lần đầu
        diff = abs(a["yes"] - prev) if prev is not None else _POLY_CHANGE_THRESHOLD
        if diff >= _POLY_CHANGE_THRESHOLD:
            changed.append({**a, "_diff": diff, "_prev": prev})
        new_prev[key] = a["yes"]   # luôn cập nhật giá trị mới nhất

    hot = data.get("hot", [])

    if not changed and not hot:
        return "", new_prev

    lines = [
        "\n\n🎯 **POLYMARKET — THỊ TRƯỜNG DỰ BÁO TOÀN CẦU**",
        "_(Xác suất từ hàng triệu USD đặt cược thực tế)_",
    ]

    # ── Anchor block (chỉ khi có thay đổi) ───────────────────────────────────
    if changed:
        lines.append("\n**📊 Chỉ số biến động hôm nay:**\n")
        for a in changed:
            bar    = _poly_bar(a["yes"] / 100)
            vol_m  = a["vol"] / 1_000_000 if a["vol"] else 0
            prev   = a["_prev"]
            diff   = a["_diff"]
            if prev is None:
                trend = ""
            elif a["yes"] > prev:
                trend = f" _(+{diff}%)_"
            else:
                trend = f" _(-{diff}%)_"
            # Hiển thị tên nhóm + câu hỏi thực tế (truncate 70 ký tự)
            q_short = a["question"][:70] + ("…" if len(a["question"]) > 70 else "")
            lines.append(
                f"• {a['emoji']} **{a['display']}**{trend}: _{q_short}_\n"
                f"  YES **{a['yes']}%** {bar} NO {a['no']}%  _(tổng vol: ${vol_m:.1f}M)_"
            )

    # ── Hot markets (luôn hiển thị) ───────────────────────────────────────────
    if hot:
        lines.append("\n**🔥 Đang hot 24h gần nhất:**\n")
        for i, m in enumerate(hot, 1):
            vol_m = m["vol"] / 1_000_000 if m["vol"] else 0   # đổi sang tổng vol
            q = m["question"]
            if len(q) > 80:
                q = q[:77] + "..."
            lines.append(
                f"{i}. {q}\n"
                f"   YES **{m['yes']}%** / NO {m['no']}%  _(tổng vol: ${vol_m:.1f}M)_"
            )

    lines.append("\n_Nguồn: polymarket.com_")
    return "\n".join(lines), new_prev


# ─── Send Telegram ────────────────────────────────────────────────────────────
def md_to_html(text: str) -> str:
    """Chuyển Markdown từ Gemini sang HTML cho Telegram (parse_mode=HTML)
    — bền hơn Markdown vì không bị break bởi dấu * lẻ trong bullet."""
    # 1. Escape ký tự HTML đặc biệt trước khi insert tag
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # 2. Link [text](url) → <a href="url">text</a>
    text = re.sub(
        r"\[([^\]]+)\]\(([^)]+)\)",
        lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>',
        text,
    )

    # 3. **bold** → <b>bold</b>  (cho phép xuống dòng trong bold)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text, flags=re.DOTALL)

    # 4. *bold* (single-star) → <b>bold</b>  (chỉ khi không phải bullet đầu dòng)
    text = re.sub(
        r"(?<![*\w])\*([^*\n][^*\n]*?)\*(?![*\w])",
        r"<b>\1</b>",
        text,
    )

    # 5. Bullet `*   ` đầu dòng → `•  ` để tránh nhầm với bold
    text = re.sub(r"^[ \t]*\*[ \t]+", "•  ", text, flags=re.MULTILINE)

    return text


def send_telegram(text: str) -> None:
    MAX = 4000
    text = md_to_html(text)
    parts = []
    while len(text) > MAX:
        split_at = text.rfind("\n\n", 0, MAX)
        if split_at == -1: split_at = MAX
        parts.append(text[:split_at])
        text = text[split_at:].lstrip()
    parts.append(text)

    for idx, part in enumerate(parts, 1):
        sent_ok = False
        for attempt in range(3):   # retry tối đa 3 lần nếu timeout
            try:
                r = requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={"chat_id": CHAT_ID, "text": part, "parse_mode": "HTML",
                          "disable_web_page_preview": True},
                    timeout=30,    # tăng lên 30s (digest dài → server xử lý lâu hơn)
                )
                if r.json().get("ok"):
                    sent_ok = True
                    break
                # Lỗi parse HTML → fallback plain text ngay, không retry
                print(f"  ⚠️  Telegram HTML lỗi (phần {idx}): {r.json().get('description')}")
                plain = re.sub(r"<[^>]+>", "", part)
                plain = plain.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={"chat_id": CHAT_ID, "text": plain, "disable_web_page_preview": True},
                    timeout=30,
                )
                sent_ok = True
                break
            except Exception as e:
                print(f"  ⚠️  Telegram timeout/lỗi (phần {idx}, lần {attempt+1}/3): {e}")
                if attempt < 2:
                    time.sleep(10 * (attempt + 1))   # chờ 10s, 20s rồi mới retry
        if not sent_ok:
            print(f"  ❌ Bỏ qua phần {idx}/{len(parts)} sau 3 lần thất bại")


# ─── Send Email ───────────────────────────────────────────────────────────────
THREAD_KEY = "_email_thread_id_"   # key đặc biệt lưu trong sent_urls.json


def get_or_create_thread_id(sent: dict) -> str:
    """Lấy Message-ID gốc của thread. Tạo mới nếu chưa có (lần chạy đầu tiên)."""
    if THREAD_KEY not in sent:
        sent[THREAD_KEY] = f"<tapro-digest-root-{uuid.uuid4().hex}@gmail.com>"
    return sent[THREAD_KEY]


def send_email(markdown_text: str, thread_id: str) -> None:
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT]):
        print("  ⚠️  Bỏ qua gửi email: chưa cấu hình EMAIL_SENDER / EMAIL_APP_PASSWORD / EMAIL_RECIPIENT")
        return
    try:
        today = now_vn().strftime("%d/%m/%Y")
        hour  = now_vn().strftime("%H:%M")

        # Subject cố định → Gmail gom tất cả vào 1 thread
        subject = "📊 TApro — Bản tin thị trường"

        # Message-ID riêng cho email này
        this_msg_id = f"<tapro-{int(time.time())}-{uuid.uuid4().hex[:8]}@gmail.com>"

        # Chuyển Markdown → HTML
        body_html = md_to_html(markdown_text)

        html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    body      {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                 font-size: 15px; background: #f5f5f5; margin: 0; padding: 20px; color: #1a1a1a; }}
    .card     {{ background: #fff; border-radius: 12px; max-width: 680px;
                 margin: 0 auto; padding: 32px 36px; box-shadow: 0 2px 8px rgba(0,0,0,.08); }}
    .timestamp{{ font-size: 14px; color: #888; margin-bottom: 20px; }}
    b         {{ color: #111; }}
    a         {{ color: #1a73e8; text-decoration: none; font-weight: 500; }}
    a:hover   {{ text-decoration: underline; }}
    p         {{ font-size: 15px; line-height: 1.8; margin: 10px 0; }}
    .footer   {{ font-size: 13px; color: #bbb; margin-top: 28px; text-align: center; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="timestamp">🕐 {today} · {hour} (GMT+7)</div>
    {body_html.replace(chr(10), '<br>')}
    <div class="footer">TApro Daily Digest · Powered by Gemini AI</div>
  </div>
</body>
</html>"""

        # Hỗ trợ nhiều email: EMAIL_RECIPIENT có thể là "a@gmail.com,b@gmail.com,..."
        recipients = [e.strip() for e in EMAIL_RECIPIENT.split(",") if e.strip()]

        msg = MIMEMultipart("alternative")
        msg["Subject"]    = subject
        msg["From"]       = f"Tin tức hàng ngày <{EMAIL_SENDER}>"
        msg["To"]         = ", ".join(recipients)
        msg["Message-ID"] = this_msg_id
        # Gom vào 1 thread bằng cách reference về message gốc
        msg["In-Reply-To"] = thread_id
        msg["References"]  = thread_id
        msg.attach(MIMEText(html, "html", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
            smtp.sendmail(EMAIL_SENDER, recipients, msg.as_string())
        print(f"  ✅ Đã gửi email → {len(recipients)} địa chỉ: {', '.join(recipients)}")
    except Exception as e:
        print(f"  ⚠️  Gửi email thất bại: {e}")


# ─── Dedup giữa các lần chạy ──────────────────────────────────────────────────
SENT_FILE = "sent_urls.json"
SENT_RETAIN_DAYS = 3  # nhớ URL đã gửi trong 3 ngày


def load_sent() -> dict:
    """Đọc danh sách URL đã gửi, tự loại bỏ entry quá cũ.
    Giữ lại các key đặc biệt (bắt đầu bằng _) như _email_thread_id_."""
    try:
        with open(SENT_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    cutoff = time.time() - SENT_RETAIN_DAYS * 86400
    result = {}
    for k, v in data.items():
        if k.startswith("_"):          # key đặc biệt → giữ mãi mãi
            result[k] = v
        elif isinstance(v, (int, float)) and v >= cutoff:  # URL → prune nếu cũ
            result[k] = v
    return result


def save_sent(sent: dict) -> None:
    with open(SENT_FILE, "w", encoding="utf-8") as f:
        json.dump(sent, f, ensure_ascii=False, indent=2)


# ─── Main pipeline ────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"[{now_vn().strftime('%H:%M:%S')}] Bắt đầu pipeline...")
    sent = load_sent()
    thread_id = get_or_create_thread_id(sent)  # lấy/tạo thread ID cho email
    url_count = sum(1 for k in sent if not k.startswith("_"))
    print(f"  Đã có {url_count} URL trong lịch sử (3 ngày gần nhất)")

    # Tier 1: Thu thập URL song song
    collectors = {
        "VnEconomy": collect_vneconomy,
        "CafeF":     collect_cafef,
        "SaigonTimes": collect_sgt,
        "NhipCauDauTu": collect_ncdt,
        "BaoDauTu":  collect_bdt,
        "BBW":       collect_bbw,
    }
    sources: dict[str, list[str]] = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fn): name for name, fn in collectors.items()}
        for f in as_completed(futs):
            name = futs[f]
            try: sources[name] = f.result()
            except Exception: sources[name] = []
    total_urls = sum(len(v) for v in sources.values())
    # Loại bỏ URL đã gửi ở các lần chạy gần đây
    for src in sources:
        sources[src] = [u for u in sources[src] if u not in sent]
    after_dedup = sum(len(v) for v in sources.values())
    print(f"  Tier1: {total_urls} URLs ({after_dedup} sau khi loại trùng) ({time.time()-t0:.1f}s)")

    # Tier 2: Fetch + filter (tối đa 15 bài/nguồn)
    tier2: list[dict] = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs2 = []
        for src, urls in sources.items():
            for u in urls[:15]:
                futs2.append(ex.submit(process, src, u))
        for f in as_completed(futs2):
            try:
                r = f.result()
                if r: tier2.append(r)
            except Exception:
                pass
    print(f"  Tier2: {len(tier2)} bài qua filter ({time.time()-t0:.1f}s)")
    tier2 = tier2[:60]  # cap 60 bài — rate limiter _gemini_wait() giữ đúng 15 RPM

    if not tier2:
        send_telegram("⚠️ Hôm nay không có bài nào qua filter.")
        return

    # Tier 3: Gemini score (2 workers, delay 2s — tránh 429)
    scored: list[dict] = []
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs3 = {ex.submit(score, art): art for art in tier2}
        for f in as_completed(futs3):
            try:
                s = f.result()
                if s:
                    art = futs3[f]
                    art["score"] = s
                    scored.append(art)
            except Exception:
                pass
    print(f"  Tier3: {len(scored)} bài scored ({time.time()-t0:.1f}s)")

    # Dedup cross-source
    def title_key(t): return re.sub(r"[^\w\s]", "", t.lower())
    clusters: list[list[dict]] = []
    for art in sorted(scored, key=lambda x: -x["score"]["total"]):
        tk = title_key(art["title"])
        placed = False
        for c in clusters:
            if difflib.SequenceMatcher(None, tk, title_key(c[0]["title"])).ratio() > 0.50:
                c.append(art); placed = True; break
        if not placed: clusters.append([art])
    final = [c[0] for c in clusters]
    final.sort(key=lambda x: -x["score"]["total"])

    # Lấy bài đáng đọc (full summary) và bài tham khảo (snippet)
    top  = [a for a in final if a["score"]["total"] >= MIN_SCORE][:12]
    refs = [a for a in final if a["score"]["total"] == MIN_SCORE - 1][:8]  # điểm 6
    print(f"  Top bài (>={MIN_SCORE} điểm): {len(top)} bài | Tham khảo: {len(refs)} bài")

    if not top:
        send_telegram(f"⚠️ Hôm nay không có bài nào đạt ≥{MIN_SCORE} điểm.")
        return

    # Tạo digest chính từ Gemini
    print(f"  Đang tạo bản tin từ {len(top)} bài...")
    digest = build_digest(top)

    # Gắn thêm phần Tin tham khảo — chèn TRƯỚC phần Rủi ro & Gợi ý
    if refs:
        ref_lines = ["\n\n📎 **TIN THAM KHẢO**\n"]
        for a in refs:
            summary = a["score"].get("summary", "").strip()
            ref_lines.append(
                f"• **{a['title']}**\n"
                f"  {summary}\n"
                f"  [đọc thêm]({a['url']})\n"
            )
        ref_block = "\n".join(ref_lines)

        # Tìm vị trí phần ⚠️ RỦI RO để chèn refs trước nó
        split_marker = None
        for marker in ["⚠️", "⚠"]:
            idx = digest.find(marker)
            if idx != -1:
                split_marker = idx
                break

        if split_marker is not None:
            digest = digest[:split_marker].rstrip() + ref_block + "\n\n" + digest[split_marker:]
        else:
            digest += ref_block  # fallback: nếu không tìm thấy thì append cuối

    # Polymarket — ghép vào cuối digest
    print("  Đang lấy dữ liệu Polymarket...")
    try:
        # Đọc anchor YES% từ lần chạy trước (lưu trong sent_urls.json)
        prev_anchors = {
            k[len("_poly_"):]: v
            for k, v in sent.items()
            if k.startswith("_poly_")
        }
        poly_data    = fetch_polymarket()
        poly_section, new_anchors = build_polymarket_section(poly_data, prev_anchors)
        if poly_section:
            digest += poly_section
            n_changed = sum(1 for a in poly_data["anchors"]
                            if abs(a["yes"] - prev_anchors.get(a["display"], -99)) >= _POLY_CHANGE_THRESHOLD)
            print(f"  Polymarket: {n_changed} anchor thay đổi, {len(poly_data['hot'])} hot")
        else:
            print("  Polymarket: không có dữ liệu / anchor chưa thay đổi (bỏ qua)")
        # Lưu giá trị anchor mới nhất vào sent để so sánh lần sau
        for display, yes_pct in new_anchors.items():
            sent[f"_poly_{display}"] = yes_pct
    except Exception as e:
        print(f"  ⚠️  Polymarket lỗi: {e}")

    # Gửi song song Telegram + Email (email dùng thread_id để gom 1 thread)
    with ThreadPoolExecutor(max_workers=2) as ex:
        ft = ex.submit(send_telegram, digest)
        fe = ex.submit(send_email, digest, thread_id)
        for label, fut in [("Telegram", ft), ("Email", fe)]:
            try:
                fut.result()
            except Exception as e:
                print(f"  ❌ {label} thất bại (không crash pipeline): {e}")

    # Cập nhật danh sách URL đã gửi (cả top lẫn refs)
    now = time.time()
    for a in top + refs:
        sent[a["url"]] = now
    save_sent(sent)
    print(f"  💾 Đã lưu {len(top)+len(refs)} URL vào {SENT_FILE} (tổng: {len(sent)} URL)")
    print(f"  ✅ Gửi xong! Tổng: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()

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
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html.parser import HTMLParser

import requests

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


class _Parser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_h1 = self.in_p = self.in_title = False
        self.title = ""
        self.content: list[str] = []
        self.description = ""

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
    return {
        "url": url,
        "title": p.title or "",
        "description": p.description or "",
        "word_count": len(body.split()),
        "content_first_500": body[:500],
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
def process(src: str, url: str) -> dict | None:
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
    time.sleep(0.3)  # rate-limit: tránh spam API từ nhiều worker cùng lúc
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, params={"key": GEMINI_KEY}, timeout=20)
            if r.status_code == 429:
                time.sleep(15 * (attempt + 1))
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
    articles_text = ""
    for i, a in enumerate(articles, 1):
        content = fetch_full_text(a["url"])
        articles_text += (
            f"--- BÀI {i} ---\n"
            f"Tiêu đề: {a['title']}\n"
            f"URL: {a['url']}\n"
            f"Nguồn: {a['source']} | Chủ đề: {a['score']['topic']} | Mood: {a['score']['mood']}\n"
            f"Nội dung: {content}\n\n"
        )
    today = datetime.now().strftime("%d/%m/%Y")
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
5. Sau MỖI tin (dưới đoạn nội dung), thêm DÒNG RIÊNG: [đọc thêm](URL_THẬT_CỦA_BÀI)
   — copy URL từ mục "URL:" của bài đó. KHÔNG để placeholder, KHÔNG bỏ qua.
6. Mỗi tin 4-6 câu, trích số liệu/tên chuyên gia/dữ kiện cụ thể.
7. Không bỏ sót bài nào.

CẤU TRÚC OUTPUT:

📊 **BẢN TIN THỊ TRƯỜNG — {today}**

🇻🇳 **KINH TẾ VIỆT NAM**

**[Tiêu đề bài 1]**
[4-6 câu nội dung, BOLD mọi số liệu]
[đọc thêm](URL_BÀI_1)

**[Tiêu đề bài 2]**
[...]
[đọc thêm](URL_BÀI_2)

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
    r = requests.post(url, json=payload, params={"key": GEMINI_KEY}, timeout=90)
    r.raise_for_status()
    return r.json()["candidates"][0]["content"]["parts"][0]["text"]


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
    for part in parts:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": part, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=15,
        )
        if not r.json().get("ok"):
            print(f"  ⚠️  Telegram HTML lỗi: {r.json().get('description')}")
            # Fallback: gỡ tag, gửi plain text
            plain = re.sub(r"<[^>]+>", "", part)
            plain = plain.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": plain, "disable_web_page_preview": True},
                timeout=15,
            )


# ─── Send Email ───────────────────────────────────────────────────────────────
def send_email(markdown_text: str) -> None:
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT]):
        print("  ⚠️  Bỏ qua gửi email: chưa cấu hình EMAIL_SENDER / EMAIL_APP_PASSWORD / EMAIL_RECIPIENT")
        return
    try:
        today = datetime.now().strftime("%d/%m/%Y")
        hour  = datetime.now().strftime("%H:%M")
        subject = f"📊 Bản tin thị trường — {today} ({hour})"

        # Chuyển Markdown → HTML cho phần body email
        body_html = md_to_html(markdown_text)

        # Bọc vào template HTML đẹp hơn
        html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    body      {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                 background: #f5f5f5; margin: 0; padding: 20px; color: #1a1a1a; }}
    .card     {{ background: #fff; border-radius: 12px; max-width: 680px;
                 margin: 0 auto; padding: 32px 36px; box-shadow: 0 2px 8px rgba(0,0,0,.08); }}
    h1        {{ font-size: 22px; margin: 0 0 24px; color: #111; }}
    b         {{ color: #111; }}
    a         {{ color: #1a73e8; text-decoration: none; font-weight: 500; }}
    a:hover   {{ text-decoration: underline; }}
    hr        {{ border: none; border-top: 1px solid #eee; margin: 24px 0; }}
    p         {{ line-height: 1.7; margin: 8px 0; }}
    .footer   {{ font-size: 12px; color: #999; margin-top: 28px; text-align: center; }}
  </style>
</head>
<body>
  <div class="card">
    {body_html.replace(chr(10), '<br>')}
    <div class="footer">TApro Daily Digest · {today} {hour} · Powered by Gemini AI</div>
  </div>
</body>
</html>"""

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"TApro Digest <{EMAIL_SENDER}>"
        msg["To"]      = EMAIL_RECIPIENT
        msg.attach(MIMEText(html, "html", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
            smtp.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
        print(f"  ✅ Đã gửi email → {EMAIL_RECIPIENT}")
    except Exception as e:
        print(f"  ⚠️  Gửi email thất bại: {e}")


# ─── Dedup giữa các lần chạy ──────────────────────────────────────────────────
SENT_FILE = "sent_urls.json"
SENT_RETAIN_DAYS = 3  # nhớ URL đã gửi trong 3 ngày


def load_sent() -> dict:
    """Đọc danh sách URL đã gửi, tự loại bỏ entry quá cũ."""
    try:
        with open(SENT_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    cutoff = time.time() - SENT_RETAIN_DAYS * 86400
    return {u: ts for u, ts in data.items() if isinstance(ts, (int, float)) and ts >= cutoff}


def save_sent(sent: dict) -> None:
    with open(SENT_FILE, "w", encoding="utf-8") as f:
        json.dump(sent, f, ensure_ascii=False, indent=2)


# ─── Main pipeline ────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Bắt đầu pipeline...")
    sent = load_sent()
    print(f"  Đã có {len(sent)} URL trong lịch sử (3 ngày gần nhất)")

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
    tier2 = tier2[:60]  # cap để Tier3 không bị quá tải

    if not tier2:
        send_telegram("⚠️ Hôm nay không có bài nào qua filter.")
        return

    # Tier 3: Gemini score (3 workers, delay 0.5s)
    scored: list[dict] = []
    with ThreadPoolExecutor(max_workers=3) as ex:
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

    # Lấy bài đáng đọc
    top = [a for a in final if a["score"]["total"] >= MIN_SCORE][:12]
    print(f"  Top bài (>={MIN_SCORE} điểm): {len(top)} bài")

    if not top:
        send_telegram(f"⚠️ Hôm nay không có bài nào đạt ≥{MIN_SCORE} điểm.")
        return

    # Tạo digest + gửi song song Telegram + Email
    print(f"  Đang tạo bản tin từ {len(top)} bài...")
    digest = build_digest(top)
    with ThreadPoolExecutor(max_workers=2) as ex:
        ft = ex.submit(send_telegram, digest)
        fe = ex.submit(send_email, digest)
        ft.result()
        fe.result()

    # Cập nhật danh sách URL đã gửi
    now = time.time()
    for a in top:
        sent[a["url"]] = now
    save_sent(sent)
    print(f"  💾 Đã lưu {len(top)} URL vào {SENT_FILE} (tổng: {len(sent)} URL)")
    print(f"  ✅ Gửi Telegram xong! Tổng: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()

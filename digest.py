"""
TApro Daily Digest — chạy độc lập, không cần DB.
Crawl tin → Gemini score → Tạo bản tin → Gửi Telegram.
"""
import difflib
import gzip
import json
import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html.parser import HTMLParser

import requests

# ─── Config (đọc từ env / GitHub Secrets) ────────────────────────────────────
GEMINI_KEY   = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-lite")
BOT_TOKEN    = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID      = os.environ["TELEGRAM_CHAT_ID"]
MIN_SCORE    = int(os.getenv("MIN_SCORE", "7"))

HDR = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
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
    out: set[str] = set()
    for cat in ["vi-mo-dau-tu","doanh-nghiep","thi-truong-chung-khoan","tai-chinh-quoc-te","tai-chinh-ngan-hang"]:
        html = fetch(f"https://cafef.vn/{cat}.chn")
        if not html: continue
        for u in re.findall(r'href="(/[^"]*-\d{8,}\.chn)"', html):
            out.add("https://cafef.vn" + u)
    SKIP = ["xon-xao","la-lung","ky-la","gay-soc","soc-","-nho-duoc","khien","phat-hien","tu-vong"]
    return [u for u in out if not any(k in u for k in SKIP)]


def collect_sgt() -> list[str]:
    out: set[str] = set()
    for cat in ["kinh-doanh","dia-oc","lang-kinh","the-gioi","doanh-nhan-doanh-nghiep"]:
        html = fetch(f"https://thesaigontimes.vn/{cat}/")
        if not html: continue
        for u in re.findall(r'href="(https://thesaigontimes\.vn/[a-z0-9\-]+/)"', html):
            if "/category/" in u or u.endswith(f"/{cat}/"): continue
            if len(u.rstrip("/").split("/")[-1].split("-")) < 5: continue
            out.add(u)
    return list(out)


def collect_ncdt() -> list[str]:
    out: set[str] = set()
    for cat in ["","kinh-doanh","tai-chinh","dau-tu","vi-mo"]:
        url = f"https://nhipcaudautu.vn/{cat}/" if cat else "https://nhipcaudautu.vn/"
        html = fetch(url)
        if not html: continue
        for u in re.findall(r'href="(https://nhipcaudautu\.vn/[^"]+\.html?)"', html):
            if not any(s in u for s in ["/tag/","/category/","/author/"]):
                out.add(u)
        for u in re.findall(r'href="(/[^"]+\.html?)"', html):
            if not any(s in u for s in ["/tag/","/category/","/author/"]):
                out.add("https://nhipcaudautu.vn" + u)
    return list(out)


def collect_bdt() -> list[str]:
    out: set[str] = set()
    for cat in ["","tai-chinh-ngan-hang","dau-tu","kinh-doanh","bat-dong-san","chung-khoan","quoc-te"]:
        url = f"https://baodautu.vn/{cat}/" if cat else "https://baodautu.vn/"
        html = fetch(url)
        if not html: continue
        for u in re.findall(r'href="(https://baodautu\.vn/[^"]+\.html?)"', html):
            if not any(s in u for s in ["/tag/","/category/","/page/"]):
                out.add(u)
        for u in re.findall(r'href="(/[a-z][^"]*\.html?)"', html):
            if not any(s in u for s in ["/tag/","/category/","/page/"]):
                out.add("https://baodautu.vn" + u)
    return list(out)


def collect_bbw() -> list[str]:
    out: set[str] = set()
    for page in ["https://bbw.vn/","https://bbw.vn/kinh-doanh","https://bbw.vn/tai-chinh",
                 "https://bbw.vn/the-gioi","https://bbw.vn/cong-nghe","https://bbw.vn/chung-khoan"]:
        html = fetch(page)
        if not html: continue
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
            f"Nguồn: {a['source']} | Chủ đề: {a['score']['topic']} | Mood: {a['score']['mood']}\n"
            f"Nội dung: {content}\n\n"
        )
    today = datetime.now().strftime("%d/%m/%Y")
    prompt = f"""Bạn là chuyên gia phân tích đầu tư chứng khoán Việt Nam.
Dưới đây là nội dung {len(articles)} bài báo chất lượng cao ngày {today}:

{articles_text}

Viết BẢN TIN BUỔI SÁNG chi tiết. Yêu cầu:
- Mỗi tin: 4-6 câu, trích dẫn số liệu/tên chuyên gia/dữ kiện cụ thể từ bài báo
- Không bỏ sót bài nào. Dùng **bold** cho tiêu đề mỗi tin.

Cấu trúc:

📊 **BẢN TIN THỊ TRƯỜNG — {today}**

━━━ 🇻🇳 KINH TẾ VIỆT NAM ━━━
[Từng bài VN - tiêu đề in đậm + 4-6 câu]

━━━ 🌍 VĨ MÔ THẾ GIỚI ━━━
[Từng bài thế giới - tiêu đề in đậm + 4-6 câu]

━━━ ⚠️ RỦI RO CẦN THEO DÕI ━━━
[3 điểm rủi ro có số liệu cụ thể]

━━━ 💡 GỢI Ý NGÀNH/CỔ PHIẾU ━━━
[Cụ thể, có lý do rõ ràng từ tin trên]"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}],
               "generationConfig": {"temperature": 0.3, "maxOutputTokens": 4000}}
    r = requests.post(url, json=payload, params={"key": GEMINI_KEY}, timeout=90)
    r.raise_for_status()
    return r.json()["candidates"][0]["content"]["parts"][0]["text"]


# ─── Send Telegram ────────────────────────────────────────────────────────────
def send_telegram(text: str) -> None:
    MAX = 4000
    text = re.sub(r"\*\*(.+?)\*\*", r"*\1*", text)
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
            json={"chat_id": CHAT_ID, "text": part, "parse_mode": "Markdown"},
            timeout=15,
        )
        if not r.json().get("ok"):
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": part},
                timeout=15,
            )


# ─── Main pipeline ────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Bắt đầu pipeline...")

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
    print(f"  Tier1: {total_urls} URLs ({time.time()-t0:.1f}s)")

    # Tier 2: Fetch + filter (tối đa 25 bài/nguồn)
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

    # Tạo digest + gửi
    print(f"  Đang tạo bản tin từ {len(top)} bài...")
    digest = build_digest(top)
    send_telegram(digest)
    print(f"  ✅ Gửi Telegram xong! Tổng: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()

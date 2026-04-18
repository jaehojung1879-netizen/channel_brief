"""
은행연합회(KFB) · 금융위원회(FSC) 공시/보도자료 수집

- 기존 직접 스크래핑은 사이트 구조 변경으로 실패 → Google News RSS
  의 site: 필터로 간접 수집하는 방식으로 교체.
- 직접 URL 호출도 candidate로 유지하되, 실패 시 RSS로 자동 fallback.
- 채널/점포/접근성 등 우선 키워드 항목은 priority=True로 표시.
"""
import feedparser
import requests
import json
import re
import time
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote
from bs4 import BeautifulSoup

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}

PRIORITY_KEYWORDS = [
    "점포", "영업점", "채널", "ATM", "키오스크", "공동", "접근성",
    "고령", "장애", "디지털", "비대면", "통계", "공시", "상생",
]

# 사이트 직접 호출 후보(1차 시도)
KFB_DIRECT_CANDIDATES = [
    "https://www.kfb.or.kr/news/press_list.php",
    "https://www.kfb.or.kr/publicdata/press_list.php",
    "https://www.kfb.or.kr/about/press_list.php",
]
FSC_DIRECT_CANDIDATES = [
    "https://www.fsc.go.kr/no010101",
    "https://www.fsc.go.kr/no010101/list",
]

# Google News RSS 쿼리 (2차 fallback) — site: 필터로 공식 사이트 기사만
KFB_RSS_QUERIES = [
    "site:kfb.or.kr",
    "은행연합회 보도자료",
    "은행연합회 발표",
]
FSC_RSS_QUERIES = [
    "site:fsc.go.kr",
    "금융위원회 보도자료",
    "금감원 보도자료",
]


def make_id(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]


def _clean_title(t: str) -> str:
    if not t:
        return ""
    # Google News의 " - 언론사" 접미사 제거
    if " - " in t:
        t = t.rsplit(" - ", 1)[0]
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _extract_source_from_title(t: str) -> str:
    if t and " - " in t:
        return t.rsplit(" - ", 1)[1].strip()
    return ""


def _is_recent(pub_dt: datetime, days: int = 21) -> bool:
    return pub_dt >= datetime.now(KST) - timedelta(days=days)


def fetch_via_google_rss(queries, default_source: str):
    """Google News RSS로 여러 쿼리를 순회하며 기사 수집."""
    items = []
    seen_links = set()
    for q in queries:
        url = f"https://news.google.com/rss/search?q={quote(q)}&hl=ko&gl=KR&ceid=KR:ko"
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:20]:
                raw_title = entry.get("title", "")
                link = entry.get("link", "")
                if not raw_title or not link or link in seen_links:
                    continue
                seen_links.add(link)

                title = _clean_title(raw_title)
                if len(title) < 5:
                    continue

                src = _extract_source_from_title(raw_title) or default_source

                pub = entry.get("published_parsed")
                if pub:
                    pub_dt = datetime(*pub[:6], tzinfo=timezone.utc).astimezone(KST)
                else:
                    pub_dt = datetime.now(KST)
                if not _is_recent(pub_dt):
                    continue

                items.append({
                    "id": make_id(link),
                    "title": title,
                    "link": link,
                    "date": pub_dt.strftime("%Y-%m-%d"),
                    "published": pub_dt.isoformat(),
                    "source": src,
                    "priority": any(kw in title for kw in PRIORITY_KEYWORDS),
                })
            time.sleep(0.8)
        except Exception as e:
            print(f"[rss] '{q}' error: {e}")
    return items


def fetch_kfb_direct():
    """은행연합회 사이트 직접 스크래핑 (실패 시 빈 리스트)."""
    for url in KFB_DIRECT_CANDIDATES:
        items = []
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            resp.encoding = resp.encoding or "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            # 게시판 후보 selector 순회
            rows = (soup.select("table.board tbody tr")
                    or soup.select("table tr")
                    or soup.select("ul.board_list li")
                    or soup.select(".board_list li"))

            for row in rows[:30]:
                a = row.find("a")
                if not a:
                    continue
                title = (a.get_text(strip=True) or "").strip()
                if len(title) < 5:
                    continue
                href = a.get("href", "")
                if href and not href.startswith("http"):
                    href = "https://www.kfb.or.kr/news/" + href.lstrip("/")
                text = row.get_text(" ", strip=True)
                m = re.search(r"(\d{4}[-.]\d{2}[-.]\d{2})", text)
                date_str = (m.group(1) if m else "").replace(".", "-")
                items.append({
                    "id": make_id(href or title),
                    "title": title,
                    "link": href,
                    "date": date_str,
                    "source": "은행연합회",
                    "priority": any(kw in title for kw in PRIORITY_KEYWORDS),
                })
            if items:
                print(f"[kfb] direct scrape OK ({url}): {len(items)} items")
                return items
        except Exception as e:
            print(f"[kfb] direct '{url}' error: {e}")
    print("[kfb] direct scrape empty → fallback to Google News RSS")
    return []


def fetch_fsc_direct():
    for url in FSC_DIRECT_CANDIDATES:
        items = []
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = (soup.select(".board_list li")
                     or soup.select("ul.list li")
                     or soup.select("table tbody tr")
                     or soup.select(".boardList tr"))
            for c in cards[:30]:
                a = c.find("a")
                if not a:
                    continue
                title = (a.get_text(strip=True) or "").strip()
                if len(title) < 5:
                    continue
                href = a.get("href", "")
                if href and not href.startswith("http"):
                    href = "https://www.fsc.go.kr" + ("/" if not href.startswith("/") else "") + href
                text = c.get_text(" ", strip=True)
                m = re.search(r"(\d{4}[-.]\d{2}[-.]\d{2})", text)
                date_str = (m.group(1) if m else "").replace(".", "-")
                items.append({
                    "id": make_id(href or title),
                    "title": title,
                    "link": href,
                    "date": date_str,
                    "source": "금융위원회",
                    "priority": any(kw in title for kw in PRIORITY_KEYWORDS),
                })
            if items:
                print(f"[fsc] direct scrape OK ({url}): {len(items)} items")
                return items
        except Exception as e:
            print(f"[fsc] direct '{url}' error: {e}")
    print("[fsc] direct scrape empty → fallback to Google News RSS")
    return []


def _sort_and_limit(items, limit=15):
    def sort_key(x):
        d = x.get("date", "") or ""
        d_num = int(d.replace("-", "")) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) else 0
        return (not x.get("priority", False), -d_num)
    items.sort(key=sort_key)
    # 중복 제거 (title 앞 25자 기준)
    seen = set()
    out = []
    for it in items:
        key = (it.get("title") or "")[:25]
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
        if len(out) >= limit:
            break
    return out


def main():
    print(f"[disclosure] start at {datetime.now(KST).isoformat()}")

    kfb = fetch_kfb_direct()
    if not kfb:
        kfb = fetch_via_google_rss(KFB_RSS_QUERIES, "은행연합회")

    fsc = fetch_fsc_direct()
    if not fsc:
        fsc = fetch_via_google_rss(FSC_RSS_QUERIES, "금융위원회")

    kfb = _sort_and_limit(kfb)
    fsc = _sort_and_limit(fsc)

    output = {
        "updated_at": datetime.now(KST).isoformat(),
        "kfb": kfb,
        "fsc": fsc,
    }

    out_path = DATA_DIR / "disclosure.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[disclosure] saved: kfb={len(kfb)} fsc={len(fsc)}")


if __name__ == "__main__":
    main()

"""
채널 이슈 트래커 (장차법·ATM·키오스크·접근성)
해외 리테일 뱅킹 혁신 사례 (영문 구글뉴스)
"""
import feedparser
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote
import time
import hashlib

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

ISSUE_KEYWORDS = [
    "장애인차별금지법 키오스크",
    "무인단말기 접근성",
    "ATM 공동화",
    "고령자 금융",
    "원격화상상담",
    "디지털 데스크 은행",
    "은행 점포 폐쇄 지역사회",
]

GLOBAL_KEYWORDS = [
    ("Capital One Cafe branch", "US"),
    ("JPMorgan Chase community center", "US"),
    ("retail bank branch innovation", "Global"),
    ("bank hub UK banking", "UK"),
    ("ING branch redesign", "EU"),
    ("bank branch of the future", "Global"),
]


def make_id(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", "", raw)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_google_news(query: str, hl="ko", gl="KR", ceid="KR:ko", limit=5):
    encoded = quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl={hl}&gl={gl}&ceid={ceid}"
    items = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries[:limit]:
            title_raw = entry.get("title", "")
            if " - " in title_raw:
                title, source = title_raw.rsplit(" - ", 1)
            else:
                title, source = title_raw, "News"

            published = entry.get("published_parsed")
            if published:
                pub_dt = datetime(*published[:6], tzinfo=timezone.utc).astimezone(KST)
            else:
                pub_dt = datetime.now(KST)

            items.append({
                "id": make_id(entry.get("link", title)),
                "title": clean_html(title),
                "source": clean_html(source),
                "summary": clean_html(entry.get("summary", ""))[:240],
                "link": entry.get("link", ""),
                "published": pub_dt.isoformat(),
                "query": query,
            })
    except Exception as e:
        print(f"[news] error '{query}': {e}")
    return items


def categorize_issue(title: str) -> str:
    """이슈 카테고리 자동 태깅"""
    t = title
    if any(k in t for k in ["장애", "키오스크", "접근성", "장차법"]):
        return "장애인차별금지법"
    if "ATM" in t or "atm" in t:
        return "ATM"
    if any(k in t for k in ["고령", "시니어"]):
        return "고령자 금융"
    if any(k in t for k in ["원격", "화상", "디지털 데스크"]):
        return "디지털 데스크"
    if any(k in t for k in ["폐쇄", "지역사회", "점포 축소"]):
        return "점포 축소"
    return "기타"


def main():
    print(f"[issue+global] start at {datetime.now(KST).isoformat()}")

    # 국내 채널 이슈
    issue_items = []
    for q in ISSUE_KEYWORDS:
        items = fetch_google_news(q, limit=4)
        issue_items.extend(items)
        time.sleep(1.0)

    # 중복 제거 + 카테고리 태깅
    seen = set()
    deduped = []
    for it in issue_items:
        key = it["title"][:30]
        if key in seen:
            continue
        seen.add(key)
        it["category"] = categorize_issue(it["title"])
        deduped.append(it)

    cutoff = datetime.now(KST) - timedelta(days=7)
    deduped = [x for x in deduped if datetime.fromisoformat(x["published"]) >= cutoff]
    deduped.sort(key=lambda x: x["published"], reverse=True)

    # 해외 사례
    global_items = []
    for q, region in GLOBAL_KEYWORDS:
        items = fetch_google_news(q, hl="en", gl="US", ceid="US:en", limit=3)
        for it in items:
            it["region"] = region
        global_items.extend(items)
        time.sleep(1.0)

    seen_g = set()
    global_deduped = []
    for it in global_items:
        key = it["title"][:40]
        if key in seen_g:
            continue
        seen_g.add(key)
        global_deduped.append(it)

    cutoff_g = datetime.now(KST) - timedelta(days=14)
    global_deduped = [x for x in global_deduped if datetime.fromisoformat(x["published"]) >= cutoff_g]
    global_deduped.sort(key=lambda x: x["published"], reverse=True)

    # 저장
    (DATA_DIR / "issues.json").write_text(
        json.dumps({
            "updated_at": datetime.now(KST).isoformat(),
            "items": deduped[:20],
        }, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[issue] saved {len(deduped[:20])} items")

    (DATA_DIR / "global.json").write_text(
        json.dumps({
            "updated_at": datetime.now(KST).isoformat(),
            "items": global_deduped[:15],
        }, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[global] saved {len(global_deduped[:15])} items")


if __name__ == "__main__":
    main()

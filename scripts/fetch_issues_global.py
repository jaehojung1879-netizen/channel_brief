"""
채널 이슈 트래커 - 상생금융 / TECH 2컬럼 재편
해외 리테일 뱅킹 혁신 사례 유지
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

# 상생금융 — 포용·소상공인·취약계층·지역사회
SOCIAL_FINANCE_KEYWORDS = [
    "상생금융",
    "포용금융",
    "서민금융",
    "소상공인 금융지원",
    "고령자 금융",
    "취약계층 금융",
    "새희망홀씨",
    "중소기업 상생",
    "지역재투자",
    "금융접근성",
    "은행 사회공헌",
]

# TECH — AI·디지털·핀테크·인프라
TECH_KEYWORDS = [
    "은행 AI",
    "은행 생성형 AI",
    "은행 클라우드",
    "마이데이터 은행",
    "비대면 실명확인",
    "원격화상상담",
    "예금토큰",
    "CBDC 은행",
    "블록체인 은행",
    "스테이블코인 은행",
    "오픈뱅킹",
    "디지털 데스크 은행",
    "스마트 키오스크 은행",
    "핀테크 제휴",
]

GLOBAL_KEYWORDS = [
    ("Capital One Cafe branch", "US"),
    ("JPMorgan Chase community center", "US"),
    ("retail bank branch innovation", "Global"),
    ("bank hub UK banking", "UK"),
    ("ING branch redesign", "EU"),
    ("bank branch of the future", "Global"),
    ("digital bank branch redesign", "Global"),
]


def make_id(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", "", raw)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_google_news(query, hl="ko", gl="KR", ceid="KR:ko", limit=5):
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


def collect_by_keywords(keywords: list, days_limit: int = 10, limit_each: int = 4) -> list:
    """키워드 리스트로 수집 + 중복 제거 + 기간 필터 + 최신순 정렬"""
    all_items = []
    for q in keywords:
        items = fetch_google_news(q, limit=limit_each)
        all_items.extend(items)
        time.sleep(0.8)

    seen = set()
    deduped = []
    for it in all_items:
        key = it["title"][:30]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)

    cutoff = datetime.now(KST) - timedelta(days=days_limit)
    deduped = [x for x in deduped if datetime.fromisoformat(x["published"]) >= cutoff]
    deduped.sort(key=lambda x: x["published"], reverse=True)
    return deduped


def main():
    print(f"[issue+global] start at {datetime.now(KST).isoformat()}")

    social_finance = collect_by_keywords(SOCIAL_FINANCE_KEYWORDS, days_limit=10, limit_each=4)
    print(f"[social_finance] {len(social_finance)} items")

    tech = collect_by_keywords(TECH_KEYWORDS, days_limit=10, limit_each=4)
    print(f"[tech] {len(tech)} items")

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

    cutoff_g = datetime.now(KST) - timedelta(days=21)
    global_deduped = [x for x in global_deduped if datetime.fromisoformat(x["published"]) >= cutoff_g]
    global_deduped.sort(key=lambda x: x["published"], reverse=True)

    # 저장
    (DATA_DIR / "issues.json").write_text(
        json.dumps({
            "updated_at": datetime.now(KST).isoformat(),
            "social_finance": social_finance[:8],
            "tech": tech[:8],
        }, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[issues] saved social_finance={len(social_finance[:8])}, tech={len(tech[:8])}")

    (DATA_DIR / "global.json").write_text(
        json.dumps({
            "updated_at": datetime.now(KST).isoformat(),
            "items": global_deduped[:12],
        }, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[global] saved {len(global_deduped[:12])} items")


if __name__ == "__main__":
    main()

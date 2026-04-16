"""
채널전략부 Daily Brief - 뉴스 스크래퍼
네이버뉴스 검색 RSS + 구글뉴스 RSS에서 영업점/채널 관련 기사 수집
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
DATA_DIR.mkdir(exist_ok=True)

# 검색 키워드 - 영업점/채널 관련
KEYWORDS = {
    "branch_news": [
        "은행 점포",
        "은행 영업점",
        "공동점포",
        "디지털 라운지",
        "스마트 점포",
        "은행 폐쇄",
        "편의점 은행",
    ],
    "policy_news": [
        "금융위 영업점",
        "금감원 점포",
        "은행연합회 공시",
        "장애인차별금지법 키오스크",
        "ATM 수수료",
    ],
}


def clean_html(raw: str) -> str:
    """HTML 태그 제거"""
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", "", raw)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def make_id(url: str) -> str:
    """URL 기반 고유 ID"""
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:12]


def fetch_google_news(query: str, max_items: int = 10) -> list:
    """구글뉴스 RSS 검색"""
    encoded = quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"
    try:
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[:max_items]:
            # 구글뉴스는 "제목 - 매체명" 포맷
            title_raw = entry.get("title", "")
            if " - " in title_raw:
                title, source = title_raw.rsplit(" - ", 1)
            else:
                title, source = title_raw, "Google News"

            published = entry.get("published_parsed")
            if published:
                pub_dt = datetime(*published[:6], tzinfo=timezone.utc).astimezone(KST)
            else:
                pub_dt = datetime.now(KST)

            items.append({
                "id": make_id(entry.get("link", title)),
                "title": clean_html(title),
                "source": clean_html(source),
                "summary": clean_html(entry.get("summary", ""))[:200],
                "link": entry.get("link", ""),
                "published": pub_dt.isoformat(),
                "query": query,
            })
        return items
    except Exception as e:
        print(f"[ERR] google news '{query}': {e}")
        return []


def dedupe(items: list) -> list:
    """제목 기반 중복 제거"""
    seen = set()
    result = []
    for item in items:
        # 제목의 앞 30자로 유사 중복 체크
        key = item["title"][:30]
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def filter_recent(items: list, days: int = 3) -> list:
    """최근 N일 내 기사만"""
    cutoff = datetime.now(KST) - timedelta(days=days)
    result = []
    for item in items:
        try:
            pub_dt = datetime.fromisoformat(item["published"])
            if pub_dt >= cutoff:
                result.append(item)
        except Exception:
            result.append(item)  # 파싱 실패 시 포함
    return result


def main():
    print(f"[news] start at {datetime.now(KST).isoformat()}")
    all_results = {}

    for category, queries in KEYWORDS.items():
        bucket = []
        for q in queries:
            print(f"[news] fetching: {q}")
            items = fetch_google_news(q, max_items=8)
            bucket.extend(items)
            time.sleep(1.0)  # rate limiting

        bucket = dedupe(bucket)
        bucket = filter_recent(bucket, days=3)
        # 최신순 정렬
        bucket.sort(key=lambda x: x["published"], reverse=True)
        all_results[category] = bucket[:20]
        print(f"[news] {category}: {len(bucket[:20])} items")

    # 저장
    output = {
        "updated_at": datetime.now(KST).isoformat(),
        "categories": all_results,
    }
    out_path = DATA_DIR / "news.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[news] saved: {out_path}")


if __name__ == "__main__":
    main()

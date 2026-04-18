"""
채널전략부 Daily Brief - 뉴스 스크래퍼
점수 기반 헤드라인 자동 선정 + 카테고리별 리스트 분리 저장
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

KEYWORDS = {
    "branch_news": [
        "은행 점포",
        "은행 영업점",
        "공동점포",
        "디지털 라운지",
        "스마트 점포",
        "은행 폐쇄",
        "편의점 은행",
        "은행 공동점포",
    ],
    "policy_news": [
        "금융위 영업점",
        "금감원 점포",
        "은행연합회 공시",
        "장애인차별금지법 키오스크",
        "ATM 수수료",
        "은행 점포폐쇄 내실화",
    ],
}

# 헤드라인 점수 계산용 키워드 가중치
KEYWORD_WEIGHTS = {
    # 핵심 주제
    "영업점": 5, "점포": 5, "지점": 4, "채널": 4,
    "폐쇄": 4, "통폐합": 4, "공동점포": 6,
    "디지털 라운지": 5, "스마트 점포": 4, "라운지": 2,
    # 접근성
    "고령자": 3, "장애인": 3, "접근성": 4,
    "키오스크": 4, "ATM": 3, "무인": 3,
    # 혁신/전략
    "편의점": 3, "상생": 4, "포용": 3,
    "혁신": 2, "AI": 2, "디지털": 3,
    # 규제/정책
    "장차법": 4, "규제": 3, "금감원": 3, "금융위": 3,
    "은행연합회": 3, "내실화": 3, "모범규준": 3,
}

# 신뢰도 높은 매체 (부분 매칭)
TRUSTED_SOURCES = {
    "연합뉴스", "매일경제", "한국경제", "조선비즈", "이데일리",
    "뉴스1", "서울경제", "파이낸셜뉴스", "아시아경제", "머니투데이",
    "헤럴드경제", "뉴시스", "서울파이낸스", "더벨", "아주경제",
    "브릿지경제", "이코노미스트", "비즈니스포스트", "인베스트조선",
    "KBS", "MBC", "SBS", "YTN", "JTBC",
}

NEGATIVE_KEYWORDS = [
    "광고", "협찬", "이벤트 안내", "입찰공고", "채용공고",
    "인사발령", "사진", "포토", "영상",
]

# 해외 비관련 기사 차감용
FOREIGN_SIGNALS = ["Vietnam", "베트남", "China.", "미얀마"]


def clean_html(raw: str) -> str:
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
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:12]


def fetch_google_news(query: str, max_items: int = 10) -> list:
    encoded = quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"
    try:
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[:max_items]:
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


def score_article(article: dict, title_count_map: dict) -> float:
    """헤드라인 선정용 점수 계산

    가중치 구성:
    - 키워드 매칭 (채널전략부 관점 연관성)
    - 최신성 (24시간 이내 가산)
    - 매체 신뢰도
    - 다중 쿼리 등장 (여러 검색어에서 잡힌 기사 = 화제성 지표)
    - 광고성·비관련 차감
    """
    score = 0.0
    title = article.get("title", "")
    source = article.get("source", "")

    # 1. 키워드 가중치
    for kw, weight in KEYWORD_WEIGHTS.items():
        if kw in title:
            score += weight

    # 2. 최신성
    try:
        pub_dt = datetime.fromisoformat(article["published"])
        now = datetime.now(pub_dt.tzinfo)
        hours_old = (now - pub_dt).total_seconds() / 3600
        if hours_old < 6:
            score += 6
        elif hours_old < 12:
            score += 4
        elif hours_old < 24:
            score += 3
        elif hours_old < 48:
            score += 1
    except Exception:
        pass

    # 3. 신뢰도 있는 매체
    if any(ts in source for ts in TRUSTED_SOURCES):
        score += 3

    # 4. 다중 쿼리 등장 (연관성 지표)
    title_key = title[:20]
    multi_count = title_count_map.get(title_key, 1)
    if multi_count > 1:
        score += (multi_count - 1) * 3

    # 5. 광고성 차감
    for kw in NEGATIVE_KEYWORDS:
        if kw in title:
            score -= 15
            break

    # 6. 해외 비관련 차감
    if any(kw in title for kw in FOREIGN_SIGNALS):
        if "한국" not in title and "국내" not in title:
            score -= 5

    # 7. 비정상 제목 차감
    if len(title) < 15:
        score -= 2
    if len(title) > 100:
        score -= 1

    return score


def dedupe(items: list) -> list:
    seen = set()
    result = []
    for item in items:
        key = item["title"][:30]
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def filter_recent(items: list, days: int = 3) -> list:
    cutoff = datetime.now(KST) - timedelta(days=days)
    result = []
    for item in items:
        try:
            pub_dt = datetime.fromisoformat(item["published"])
            if pub_dt >= cutoff:
                result.append(item)
        except Exception:
            result.append(item)
    return result


def main():
    print(f"[news] start at {datetime.now(KST).isoformat()}")
    all_results = {}
    raw_pool = []  # 헤드라인 점수 계산용

    for category, queries in KEYWORDS.items():
        bucket = []
        for q in queries:
            print(f"[news] fetching: {q}")
            items = fetch_google_news(q, max_items=8)
            bucket.extend(items)
            raw_pool.extend(items)
            time.sleep(1.0)

        bucket = filter_recent(bucket, days=3)
        bucket = dedupe(bucket)
        bucket.sort(key=lambda x: x["published"], reverse=True)
        all_results[category] = bucket[:20]
        print(f"[news] {category}: {len(bucket[:20])} items")

    # 전체 풀에서 제목 등장 횟수 집계 (중복도 = 화제성)
    title_count_map = {}
    for a in raw_pool:
        key = a.get("title", "")[:20]
        title_count_map[key] = title_count_map.get(key, 0) + 1

    # 헤드라인 후보: 중복 제거된 전체 리스트
    combined = []
    for cat_items in all_results.values():
        combined.extend(cat_items)
    combined = dedupe(combined)

    # 점수 계산
    scored = [
        {"article": a, "score": score_article(a, title_count_map)}
        for a in combined
    ]
    scored.sort(key=lambda x: x["score"], reverse=True)

    print("[news] top 5 headline candidates:")
    for i, s in enumerate(scored[:5]):
        print(f"  #{i+1} [{s['score']:.1f}] {s['article']['title'][:60]}")

    headline = None
    if scored and scored[0]["score"] > 0:
        headline = {**scored[0]["article"], "_score": round(scored[0]["score"], 1)}

    output = {
        "updated_at": datetime.now(KST).isoformat(),
        "headline": headline,
        "categories": all_results,
    }
    out_path = DATA_DIR / "news.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[news] saved: {out_path}")


if __name__ == "__main__":
    main()

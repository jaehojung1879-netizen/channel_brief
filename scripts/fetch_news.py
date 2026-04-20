"""
채널전략부 Daily Brief - 뉴스 스크래퍼
점수 기반 헤드라인 자동 선정 + 카테고리별 리스트 분리 저장
표시 대상 기사에 대해 og:description 수집 + (옵션) Claude 요약 생성
"""
import feedparser
import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote
import time
import hashlib

import requests
from bs4 import BeautifulSoup

try:
    import anthropic  # optional
except Exception:
    anthropic = None

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

SHINHAN_KEYWORDS = [
    "신한은행 영업점",
    "신한은행 점포",
    "신한은행 지점",
]

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

BLOCKED_TITLE_KEYWORDS = [
    "cofix", "코픽스", "자금조달비용지수",
    "예금금리", "대출금리", "환율", "채권", "국채",
    "vietnam", "베트남", "미얀마", "캄보디아", "라오스",
    "로펌라운지", "법무법인", "로펌",
]

CORE_BRANCH_KEYWORDS = [
    "영업점", "점포", "지점", "공동점포", "점포폐쇄", "채널",
    "스마트점포", "디지털 라운지", "은행 점포", "은행 영업점",
]

BANK_CONTEXT_KEYWORDS = [
    "은행", "신한", "국민", "하나", "우리", "kb", "shinhan", "woori", "hana",
    "금감원", "금융위", "은행연합회",
]


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


def fetch_google_news(query: str, max_items: int = 16) -> list:
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
                "engine": "google",
                "rank": len(items) + 1,
            })
        return items
    except Exception as e:
        print(f"[ERR] google news '{query}': {e}")
        return []


def fetch_naver_news(query: str, max_items: int = 12) -> list:
    """네이버 뉴스 검색 결과에서 상위 기사 추출 (국내 섹션 헤드라인 보강)."""
    url = "https://search.naver.com/search.naver"
    try:
        r = requests.get(
            url,
            params={"where": "news", "query": query, "sort": "0", "pd": "3"},
            headers={"User-Agent": UA},
            timeout=8,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        items = []
        seen = set()
        for idx, a in enumerate(soup.select("a.news_tit"), start=1):
            if len(items) >= max_items:
                break
            title = clean_html(a.get("title") or a.get_text(" ", strip=True))
            link = a.get("href") or ""
            if not title or not link:
                continue
            key = title[:30]
            if key in seen:
                continue
            seen.add(key)
            items.append({
                "id": make_id(link),
                "title": title,
                "source": "Naver News",
                "summary": "",
                "link": link,
                "published": datetime.now(KST).isoformat(),
                "query": query,
                "engine": "naver",
                "rank": idx,
            })
        return items
    except Exception as e:
        print(f"[ERR] naver news '{query}': {e}")
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

    # 3.5 검색엔진/순위 가중치 (국내 이슈는 네이버 우선 반영)
    engine = article.get("engine", "")
    rank = int(article.get("rank", 99) or 99)
    if engine == "naver":
        score += 3
        if rank <= 3:
            score += (4 - rank) * 1.5
    elif engine == "google" and rank <= 3:
        score += (4 - rank) * 1.0

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


def is_relevant_article(item: dict, require_core: bool = True) -> bool:
    title = (item.get("title") or "").strip()
    summary = (item.get("summary") or "").strip()
    source = (item.get("source") or "").strip()
    hay = f"{title} {summary}".lower()

    if any(kw in hay for kw in BLOCKED_TITLE_KEYWORDS):
        return False
    if any(kw.lower() in source.lower() for kw in ("vietnam", "미얀마")):
        return False

    # 영업점 연관 키워드가 최소 1개는 있어야 표시
    if require_core and not any(kw.lower() in hay for kw in CORE_BRANCH_KEYWORDS):
        return False
    # 은행 맥락이 없으면 제외 (로펌 라운지 등 비관련 기사 제거)
    if not any(kw.lower() in hay for kw in BANK_CONTEXT_KEYWORDS):
        return False
    return True


def is_shinhan_article(item: dict) -> bool:
    title = (item.get("title") or "").strip()
    summary = (item.get("summary") or "").strip()
    hay = f"{title} {summary}".lower()
    if "신한" not in hay and "shinhan" not in hay:
        return False
    return is_relevant_article(item, require_core=False)


UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)


def fetch_article_snippet(url: str, timeout: float = 6.0) -> str:
    """기사 URL을 열어 og:description / meta description / 첫 단락을 추출."""
    if not url:
        return ""
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
        if r.status_code != 200 or not r.text:
            return ""
        soup = BeautifulSoup(r.text, "lxml")
        for sel in [
            ("meta", {"property": "og:description"}),
            ("meta", {"name": "description"}),
            ("meta", {"name": "twitter:description"}),
        ]:
            tag = soup.find(*sel)
            if tag and tag.get("content"):
                text = clean_html(tag["content"])
                if len(text) >= 20:
                    return text[:600]
        # fallback: 본문 후보 영역의 첫 단락
        for sel in ["article p", "#articleBody p", ".article-body p", ".news-body p", "main p"]:
            p = soup.select_one(sel)
            if p and p.get_text(strip=True):
                text = clean_html(p.get_text(" "))
                if len(text) >= 30:
                    return text[:600]
        # 마지막 fallback: 페이지 내 첫 번째 긴 p
        for p in soup.find_all("p"):
            text = clean_html(p.get_text(" "))
            if len(text) >= 40:
                return text[:600]
    except Exception as e:
        print(f"[news] snippet fail {url[:60]}: {e}")
    return ""


_claude_client = None
_claude_disabled = False


def _get_claude():
    global _claude_client, _claude_disabled
    if _claude_disabled:
        return None
    if _claude_client is not None:
        return _claude_client
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key or anthropic is None:
        _claude_disabled = True
        return None
    try:
        _claude_client = anthropic.Anthropic(api_key=key)
        return _claude_client
    except Exception as e:
        print(f"[news] claude init fail: {e}")
        _claude_disabled = True
        return None


def summarize_with_claude(title: str, snippet: str) -> str:
    """Claude Haiku로 2-3문장 한국어 요약. 실패 시 빈 문자열."""
    client = _get_claude()
    if not client or not snippet:
        return ""
    prompt = (
        f"다음은 한국 금융 기사입니다. 채널전략(영업점·지점) 실무자 관점에서 "
        f"핵심만 2문장, 한국어 존댓말로 요약해 주세요. 불필요한 머리말 없이 요약문만 출력하세요.\n\n"
        f"[제목]\n{title}\n\n[본문 발췌]\n{snippet}"
    )
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=220,
            messages=[{"role": "user", "content": prompt}],
        )
        parts = []
        for block in msg.content:
            if getattr(block, "type", None) == "text":
                parts.append(block.text)
        return " ".join(parts).strip()
    except Exception as e:
        print(f"[news] claude summarize fail: {e}")
        return ""


def enrich_with_summary(item: dict, budget_s: float = 4.0) -> None:
    """단일 기사 아이템에 ai_summary 필드 추가."""
    if item.get("ai_summary"):
        return
    t0 = time.time()
    snippet = fetch_article_snippet(item.get("link", ""), timeout=budget_s)
    if not snippet:
        item["ai_summary"] = ""
        return
    # Claude 요약을 시도하되, 실패 시 snippet을 그대로 사용
    summary = summarize_with_claude(item.get("title", ""), snippet)
    item["ai_summary"] = summary or snippet
    elapsed = time.time() - t0
    print(f"[news] enrich {item.get('title','')[:30]}… ({elapsed:.1f}s, {'LLM' if summary else 'snippet'})")


def filter_recent(items: list, days: int = 5) -> list:
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
            g_items = fetch_google_news(q, max_items=16)
            n_items = fetch_naver_news(q, max_items=12)
            items = g_items + n_items
            bucket.extend(items)
            raw_pool.extend(items)
            time.sleep(1.0)

        bucket = [it for it in bucket if is_relevant_article(it)]
        bucket = filter_recent(bucket, days=5)
        bucket = dedupe(bucket)
        bucket.sort(key=lambda x: x["published"], reverse=True)
        all_results[category] = bucket[:40]
        print(f"[news] {category}: {len(bucket[:40])} items")

    # 신한은행 전용 풀(최소 2개 노출 보장용)
    shinhan_bucket = []
    for q in SHINHAN_KEYWORDS:
        print(f"[news] fetching(shinhan): {q}")
        g_items = fetch_google_news(q, max_items=14)
        n_items = fetch_naver_news(q, max_items=10)
        shinhan_bucket.extend(g_items + n_items)
        raw_pool.extend(g_items + n_items)
        time.sleep(1.0)
    shinhan_bucket = [it for it in shinhan_bucket if is_shinhan_article(it)]
    shinhan_bucket = filter_recent(shinhan_bucket, days=7)
    shinhan_bucket = dedupe(shinhan_bucket)
    shinhan_bucket.sort(key=lambda x: x["published"], reverse=True)

    # 최소 2개 보장: 일반 풀에서 신한 관련 기사 보충
    if len(shinhan_bucket) < 2:
        combined_general = (all_results.get("branch_news", []) + all_results.get("policy_news", []))
        for it in combined_general:
            if not is_shinhan_article(it):
                continue
            key = (it.get("title", "")[:30], it.get("link", ""))
            seen = {(x.get("title", "")[:30], x.get("link", "")) for x in shinhan_bucket}
            if key in seen:
                continue
            shinhan_bucket.append(it)
            if len(shinhan_bucket) >= 2:
                break
    all_results["shinhan_news"] = shinhan_bucket[:20]
    print(f"[news] shinhan_news: {len(all_results['shinhan_news'])} items")

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

    # Skip yesterday's headline so the same article doesn't headline two days in a row.
    prev_headline_key = None
    out_path = DATA_DIR / "news.json"
    if out_path.exists():
        try:
            prev = json.loads(out_path.read_text(encoding="utf-8"))
            prev_h = prev.get("headline") or {}
            if prev_h.get("title"):
                prev_headline_key = prev_h["title"][:40]
        except Exception:
            pass

    headline = None
    for s in scored:
        if s["score"] <= 0:
            break
        article = s["article"]
        if prev_headline_key and article.get("title", "")[:40] == prev_headline_key:
            continue
        headline = {**article, "_score": round(s["score"], 1)}
        break

    # 화면에 실제 노출될 후보만 enrichment 대상으로 (비용·시간 절약)
    display_targets = []
    if headline:
        display_targets.append(headline)
    for cat_items in all_results.values():
        display_targets.extend(cat_items[:9])
    seen_ids = set()
    unique_targets = []
    for it in display_targets:
        iid = it.get("id")
        if iid and iid in seen_ids:
            continue
        seen_ids.add(iid)
        unique_targets.append(it)

    print(f"[news] enrichment targets: {len(unique_targets)} articles (LLM={'on' if _get_claude() else 'off'})")
    for it in unique_targets:
        enrich_with_summary(it)

    output = {
        "updated_at": datetime.now(KST).isoformat(),
        "headline": headline,
        "categories": all_results,
    }
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[news] saved: {out_path}")


if __name__ == "__main__":
    main()

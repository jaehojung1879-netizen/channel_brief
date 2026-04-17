"""
LH / 국토교통부 개발 공시 + 시중은행 점포 수 추이
개발 공시는 구글뉴스 RSS로 우회 (공식 API 없음)
점포 수는 은행연합회 소비자포털 통계를 우선 스크래핑
"""
import feedparser
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote
import time
import hashlib
import requests
from bs4 import BeautifulSoup

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}

KFB_BRANCH_URL_CANDIDATES = [
    # 실제 운영 중 URL은 사이트 개편에 따라 변경될 수 있어 후보군으로 순차 시도
    "https://portal.kfb.or.kr/fingoods/saving/listBranchStatistics.do",
    "https://portal.kfb.or.kr/fingoods/saving/branchStatistics.do",
    "https://portal.kfb.or.kr/fingoods/branch/branchStatistics.do",
    "https://www.kfb.or.kr/consumer/stat/branch.php",
    "https://exchange.kfb.or.kr/page/branch.php",
]

TARGET_BANKS = [
    {"name": "KB국민", "aliases": ["kb국민", "국민은행", "kb"]},
    {"name": "신한", "aliases": ["신한은행", "신한"]},
    {"name": "하나", "aliases": ["하나은행", "keb하나", "하나"]},
    {"name": "우리", "aliases": ["우리은행", "우리"]},
    {"name": "NH농협", "aliases": ["nh농협은행", "농협은행", "nh농협", "농협"]},
]

DEV_KEYWORDS = [
    "LH 지구지정",
    "국토교통부 고시",
    "도시재생 활성화계획",
    "복합개발 지구단위계획",
    "반도체 국가산단",
    "신도시 착공",
    "기반시설 인가",
]


def make_id(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", "", raw)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_development_news():
    """구글뉴스로 개발 공시 관련 기사 수집"""
    all_items = []
    for q in DEV_KEYWORDS:
        encoded = quote(q)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
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

                all_items.append({
                    "id": make_id(entry.get("link", title)),
                    "title": clean_html(title),
                    "source": clean_html(source),
                    "link": entry.get("link", ""),
                    "published": pub_dt.isoformat(),
                    "date": pub_dt.strftime("%Y-%m-%d"),
                    "query": q,
                })
            time.sleep(1.0)
        except Exception as e:
            print(f"[dev] error '{q}': {e}")

    # 중복 제거
    seen = set()
    result = []
    for item in all_items:
        key = item["title"][:30]
        if key in seen:
            continue
        seen.add(key)
        result.append(item)

    # 최근 7일만
    cutoff = datetime.now(KST) - timedelta(days=7)
    result = [x for x in result if datetime.fromisoformat(x["published"]) >= cutoff]
    result.sort(key=lambda x: x["published"], reverse=True)
    return result[:15]


def _norm(s: str) -> str:
    return re.sub(r"\s+", "", (s or "").strip()).lower()


def _extract_int(s: str):
    nums = re.findall(r"\d[\d,]*", s or "")
    if not nums:
        return None
    return int(nums[-1].replace(",", ""))


def _map_bank_name(raw_name: str):
    n = _norm(raw_name)
    for b in TARGET_BANKS:
        for alias in b["aliases"]:
            if _norm(alias) in n:
                return b["name"]
    return None


def _parse_as_of(text: str):
    m = re.search(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})\s*일?", text or "")
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    m2 = re.search(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})\s*월?\s*말", text or "")
    if m2:
        y, mo = m2.groups()
        return f"{int(y):04d}-{int(mo):02d}-31"
    return ""


def load_previous_stats():
    """이전 분기 대비 증감 계산을 위한 이전 값 로딩"""
    for p in [DATA_DIR / "branch_stats.json", DATA_DIR / "branch_stats_manual.json"]:
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
    return {"banks": []}


def fetch_branch_stats_from_kfb():
    """은행연합회 소비자포털/통계 페이지에서 5대 은행 점포 수 스크래핑."""
    for url in KFB_BRANCH_URL_CANDIDATES:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            whole_text = soup.get_text(" ", strip=True)
            as_of = _parse_as_of(whole_text)

            parsed = {}
            for tr in soup.select("table tr"):
                cells = [c.get_text(" ", strip=True) for c in tr.select("th,td")]
                if len(cells) < 2:
                    continue
                row_text = " ".join(cells)
                # 점포수 통계 테이블이 아닌 행은 배제
                if not any(k in row_text for k in ["점포", "영업점", "지점", "은행"]):
                    continue
                bank = _map_bank_name(cells[0])
                if not bank:
                    # 첫 열이 은행명이 아닐 수도 있어 전체 셀 순회
                    for c in cells:
                        bank = _map_bank_name(c)
                        if bank:
                            break
                if not bank:
                    continue
                count = None
                # 숫자 후보 셀 탐색
                for c in reversed(cells):
                    count = _extract_int(c)
                    if count is not None:
                        break
                if count is None:
                    continue
                parsed[bank] = count

            if len(parsed) >= 5:
                banks = [{"name": b["name"], "count": parsed[b["name"]]} for b in TARGET_BANKS]
                # 비정상 파싱 방지: 은행별/합계 범위 검증
                if not all(100 <= x["count"] <= 3000 for x in banks):
                    continue
                total = sum(x["count"] for x in banks)
                if not (1000 <= total <= 10000):
                    continue
                return {
                    "as_of": as_of or datetime.now(KST).strftime("%Y-%m-%d"),
                    "source": "은행연합회 소비자포털 점포 통계",
                    "source_url": url,
                    "banks": banks,
                }
        except Exception as e:
            print(f"[stats] kfb scrape fail ({url}): {e}")

    return None


def load_branch_stats():
    """점포 수 스크래핑 우선, 실패 시 수동 파일 fallback."""
    scraped = fetch_branch_stats_from_kfb()
    if scraped:
        return scraped

    stat_file = DATA_DIR / "branch_stats_manual.json"
    if stat_file.exists():
        fallback = json.loads(stat_file.read_text(encoding="utf-8"))
        fallback["source"] = f"{fallback.get('source', '수동 입력')} (스크래핑 실패로 fallback)"
        fallback["is_fallback"] = True
        return fallback

    return {
        "as_of": datetime.now(KST).strftime("%Y-%m-%d"),
        "source": "데이터 없음 (스크래핑 실패)",
        "banks": [{"name": b["name"], "count": 0, "delta_qoq": 0} for b in TARGET_BANKS],
        "total": 0,
        "total_delta_qoq": 0,
        "is_fallback": True,
    }


def main():
    print(f"[dev+stats] start at {datetime.now(KST).isoformat()}")

    dev_items = fetch_development_news()
    stats = load_branch_stats()

    dev_output = {
        "updated_at": datetime.now(KST).isoformat(),
        "items": dev_items,
    }
    (DATA_DIR / "development.json").write_text(
        json.dumps(dev_output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[dev] saved {len(dev_items)} items")

    prev = load_previous_stats()
    prev_map = {b.get("name"): b.get("count", 0) for b in prev.get("banks", [])}

    banks_out = []
    for b in stats.get("banks", []):
        old = prev_map.get(b["name"])
        delta_qoq = (b["count"] - old) if isinstance(old, int) else 0
        banks_out.append({
            "name": b["name"],
            "count": b["count"],
            "delta_qoq": delta_qoq,
        })

    total = sum(x["count"] for x in banks_out)
    total_prev = sum(prev_map.get(x["name"], x["count"]) for x in banks_out)

    stats_output = {
        "updated_at": datetime.now(KST).isoformat(),
        "as_of": stats.get("as_of"),
        "source": stats.get("source"),
        "source_url": stats.get("source_url", ""),
        "is_fallback": bool(stats.get("is_fallback", False)),
        "banks": banks_out,
        "total": total,
        "total_delta_qoq": total - total_prev,
    }
    (DATA_DIR / "branch_stats.json").write_text(
        json.dumps(stats_output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[stats] saved: {stats['as_of']}")


if __name__ == "__main__":
    main()

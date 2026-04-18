"""
LH / 국토교통부 개발 공시 + 시중은행 점포 수 추이
개발 공시는 구글뉴스 RSS로 우회 (공식 API 없음)
점포 수는 은행연합회 소비자포털 통계를 우선 스크래핑
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

# 금융통계정보시스템(FISIS) OpenAPI
# - 권장: GitHub Secrets에 FISIS_API_KEY 저장
# - 선택: FISIS_BRANCH_API_URL 전체 URL(키 제외) 지정 시 해당 URL 우선 사용
FISIS_DEFAULT_ENDPOINT = "http://fisis.fss.or.kr/openapi/statisticsListSearch.json"

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


def _env_first(*names, default=""):
    """주어진 환경변수 이름 중 첫 번째 유효값 반환."""
    for name in names:
        v = os.environ.get(name, "").strip()
        if v:
            return v
    return default


def fetch_branch_stats_from_fisis():
    """금융통계정보시스템(FISIS) API에서 점포 수 조회."""
    api_key = _env_first(
        "FISIS_API_KEY",
        "BRANCH_STATS_API_KEY",
        "BRANCH_API_KEY",
    )
    if not api_key:
        return None

    explicit_url = _env_first("FISIS_BRANCH_API_URL", "BRANCH_STATS_API_URL")
    finance_cd = _env_first("FISIS_FINANCE_CD", "BRANCH_STATS_FINANCE_CD", default="BK")  # 예: 은행권
    list_no = _env_first("FISIS_LIST_NO", "BRANCH_STATS_LIST_NO")
    account_cd = _env_first("FISIS_ACCOUNT_CD", "BRANCH_STATS_ACCOUNT_CD")
    term = _env_first("FISIS_TERM", "BRANCH_STATS_TERM", default="Q")  # Q / M / Y
    lang = _env_first("FISIS_LANG", "BRANCH_STATS_LANG", default="kr")
    start_ym = _env_first("FISIS_START_BASE_MM", "BRANCH_STATS_START_BASE_MM", default="202001")
    end_ym = _env_first("FISIS_END_BASE_MM", "BRANCH_STATS_END_BASE_MM", default=datetime.now(KST).strftime("%Y%m"))

    if explicit_url:
        # 예: http://fisis.fss.or.kr/openapi/statisticsListSearch.json?financeCd=BK&listNo=...&accountCd=...
        url = explicit_url
        sep = "&" if "?" in url else "?"
        req_url = f"{url}{sep}auth={api_key}&lang={lang}"
    else:
        # 통계코드(listNo/accountCd)는 발급 환경마다 다를 수 있어 환경변수로 주입
        if not list_no or not account_cd:
            return None
        req_url = (
            f"{FISIS_DEFAULT_ENDPOINT}"
            f"?auth={api_key}&lang={lang}&financeCd={finance_cd}"
            f"&listNo={list_no}&accountCd={account_cd}&term={term}"
            f"&startBaseMm={start_ym}&endBaseMm={end_ym}"
        )

    try:
        resp = requests.get(req_url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            print(f"[stats] fisis status={resp.status_code}")
            return None
        data = resp.json()
    except Exception as e:
        print(f"[stats] fisis request fail: {e}")
        return None

    # 응답 형식 호환 처리
    rows = []
    for key in ["result", "list", "data", "items"]:
        if isinstance(data.get(key), list):
            rows = data[key]
            break
    if not rows and isinstance(data.get("response"), dict):
        r = data["response"]
        if isinstance(r.get("result"), list):
            rows = r["result"]
        elif isinstance(r.get("data"), list):
            rows = r["data"]
    if not rows:
        return None

    # 가장 최신 기준월 탐색
    def ym_of(row):
        for k in ["baseYm", "base_mm", "baseMm", "basYm", "baseYymm"]:
            v = str(row.get(k, "")).strip()
            if re.fullmatch(r"\d{6}", v):
                return v
        txt = " ".join(str(v) for v in row.values())
        m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", txt)
        return "".join(m.groups()) if m else "000000"

    latest_ym = max((ym_of(r) for r in rows), default="000000")
    latest_rows = [r for r in rows if ym_of(r) == latest_ym] or rows

    parsed = {}
    for row in latest_rows:
        # 은행명 후보
        name_candidates = []
        for nk in ["financeNm", "companyNm", "finCompNm", "bankNm", "finance_name", "kor_co_nm", "name"]:
            if row.get(nk):
                name_candidates.append(str(row.get(nk)))
        if not name_candidates:
            name_candidates = [str(v) for v in row.values() if isinstance(v, str)]

        bank = None
        for n in name_candidates:
            bank = _map_bank_name(n)
            if bank:
                break
        if not bank:
            continue

        # 숫자 후보
        value = None
        for vk in ["dataValue", "value", "val", "resultVal", "amt", "cnt", "count"]:
            if row.get(vk) is not None:
                value = _extract_int(str(row.get(vk)))
                if value is not None:
                    break
        if value is None:
            for v in row.values():
                value = _extract_int(str(v))
                if value is not None:
                    break
        if value is None:
            continue
        parsed[bank] = value

    if len(parsed) < 5:
        return None

    banks = [{"name": b["name"], "count": parsed[b["name"]]} for b in TARGET_BANKS]
    if not all(100 <= x["count"] <= 3000 for x in banks):
        return None
    total = sum(x["count"] for x in banks)
    if not (1000 <= total <= 10000):
        return None

    as_of = f"{latest_ym[:4]}-{latest_ym[4:6]}-31" if re.fullmatch(r"\d{6}", latest_ym) else datetime.now(KST).strftime("%Y-%m-%d")
    return {
        "as_of": as_of,
        "source": "금융통계정보시스템(FISIS) OpenAPI",
        "source_url": explicit_url or FISIS_DEFAULT_ENDPOINT,
        "banks": banks,
    }


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
    """점포 수 API 우선, 실패 시 스크래핑/수동 파일 fallback."""
    fisis = fetch_branch_stats_from_fisis()
    if fisis:
        return fisis

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

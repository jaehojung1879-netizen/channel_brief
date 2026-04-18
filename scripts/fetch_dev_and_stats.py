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


def _fisis_get_rows(service: str, api_key: str, params: dict):
    """FISIS OpenAPI 공통 호출."""
    url = f"http://fisis.fss.or.kr/openapi/{service}.json"
    q = {"auth": api_key, "lang": "kr", **params}
    try:
        resp = requests.get(url, params=q, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            print(f"[stats] {service} status={resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        print(f"[stats] {service} request fail: {e}")
        return []

    for key in ["result", "list", "data", "items"]:
        if isinstance(data.get(key), list):
            return data[key]
    if isinstance(data.get("response"), dict):
        r = data["response"]
        if isinstance(r.get("result"), list):
            return r["result"]
        if isinstance(r.get("data"), list):
            return r["data"]
    return []


def _pick_value(row: dict, keys: list[str]):
    for k in keys:
        if row.get(k) is not None:
            return str(row.get(k)).strip()
    return ""


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
    finance_cd = _env_first("FISIS_FINANCE_CD", "BRANCH_STATS_FINANCE_CD")
    list_no = _env_first("FISIS_LIST_NO", "BRANCH_STATS_LIST_NO")
    account_cd = _env_first("FISIS_ACCOUNT_CD", "BRANCH_STATS_ACCOUNT_CD")
    term = _env_first("FISIS_TERM", "BRANCH_STATS_TERM", default="Q")  # Q / M / Y
    lang = _env_first("FISIS_LANG", "BRANCH_STATS_LANG", default="kr")
    start_ym = _env_first("FISIS_START_BASE_MM", "BRANCH_STATS_START_BASE_MM", default="202001")
    end_ym = _env_first("FISIS_END_BASE_MM", "BRANCH_STATS_END_BASE_MM", default=datetime.now(KST).strftime("%Y%m"))
    part_div = _env_first("FISIS_PART_DIV", "BRANCH_STATS_PART_DIV", default="A")  # 국내은행
    lrg_div = _env_first("FISIS_LRG_DIV", "BRANCH_STATS_LRG_DIV", default="A")     # 국내은행
    sml_div = _env_first("FISIS_SML_DIV", "BRANCH_STATS_SML_DIV", default="A")     # 일반현황

    if explicit_url:
        # 예: http://fisis.fss.or.kr/openapi/statisticsListSearch.json?financeCd=BK&listNo=...&accountCd=...
        url = explicit_url
        sep = "&" if "?" in url else "?"
        req_url = f"{url}{sep}auth={api_key}&lang={lang}"
        try:
            resp = requests.get(req_url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                print(f"[stats] fisis explicit status={resp.status_code}")
                return None
            data = resp.json()
        except Exception as e:
            print(f"[stats] fisis explicit fail: {e}")
            return None
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
    else:
        # 1) 국내은행 금융회사 목록
        companies = _fisis_get_rows("companySearch", api_key, {"partDiv": part_div, "lang": lang})
        if not companies:
            return None
        comp_map = {}
        for row in companies:
            nm = _pick_value(row, ["financeNm", "finCompNm", "companyNm", "finance_name", "name"])
            cd = _pick_value(row, ["financeCd", "finance_cd", "finCode", "code"])
            bank = _map_bank_name(nm)
            if bank and cd:
                comp_map[bank] = cd
        if len(comp_map) < 5:
            return None

        # 2) 통계코드(listNo) 탐색
        if not list_no:
            params = {"lrgDiv": lrg_div, "lang": lang}
            if sml_div:
                params["smlDiv"] = sml_div
            stat_list = _fisis_get_rows("statisticsListSearch", api_key, params)
            if not stat_list:
                return None
            cand = None
            for row in stat_list:
                name = _pick_value(row, ["listNm", "statNm", "statisticsNm", "name", "title"])
                code = _pick_value(row, ["listNo", "list_no", "statNo", "code"])
                text = f"{name} {code}"
                if code and any(k in text for k in ["점포", "영업점", "지점"]):
                    cand = code
                    break
            list_no = cand or list_no
        if not list_no:
            return None

        # 3) 계정코드(accountCd) 탐색
        if not account_cd:
            acc_list = _fisis_get_rows("accountListSearch", api_key, {"listNo": list_no, "lang": lang})
            if acc_list:
                picked = ""
                for row in acc_list:
                    nm = _pick_value(row, ["accountNm", "name", "title"])
                    cd = _pick_value(row, ["accountCd", "account_cd", "code"])
                    if cd and any(k in nm for k in ["점포", "영업점", "지점", "총계", "합계"]):
                        picked = cd
                        break
                if not picked:
                    picked = _pick_value(acc_list[0], ["accountCd", "account_cd", "code"])
                account_cd = picked

    # 가장 최신 기준월 탐색
    def ym_of(row):
        for k in ["baseYm", "base_mm", "baseMm", "basYm", "baseYymm"]:
            v = str(row.get(k, "")).strip()
            if re.fullmatch(r"\d{6}", v):
                return v
        txt = " ".join(str(v) for v in row.values())
        m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", txt)
        return "".join(m.groups()) if m else "000000"

    parsed = {}
    all_rows = []
    for b in TARGET_BANKS:
        bank_name = b["name"]
        fin_cd = finance_cd or comp_map.get(bank_name, "")
        if not fin_cd or not list_no:
            continue
        params = {
            "financeCd": fin_cd,
            "listNo": list_no,
            "term": term,
            "startBaseMm": start_ym,
            "endBaseMm": end_ym,
            "lang": lang,
        }
        if account_cd:
            params["accountCd"] = account_cd
        rows = _fisis_get_rows("statisticsInfoSearch", api_key, params)
        if not rows:
            continue
        all_rows.extend(rows)
        latest_ym = max((ym_of(r) for r in rows), default="000000")
        latest_rows = [r for r in rows if ym_of(r) == latest_ym] or rows
        val = None
        for row in latest_rows:
            for vk in ["dataValue", "value", "val", "resultVal", "amt", "cnt", "count"]:
                if row.get(vk) is not None:
                    val = _extract_int(str(row.get(vk)))
                    if val is not None:
                        break
            if val is None:
                for v in row.values():
                    val = _extract_int(str(v))
                    if val is not None:
                        break
            if val is not None:
                parsed[bank_name] = val
                break

    if len(parsed) < 5:
        return None

    banks = [{"name": b["name"], "count": parsed[b["name"]]} for b in TARGET_BANKS]
    if not all(100 <= x["count"] <= 3000 for x in banks):
        return None
    total = sum(x["count"] for x in banks)
    if not (1000 <= total <= 10000):
        return None

    latest_ym_all = max((ym_of(r) for r in all_rows), default="000000")
    as_of = f"{latest_ym_all[:4]}-{latest_ym_all[4:6]}-31" if re.fullmatch(r"\d{6}", latest_ym_all) else datetime.now(KST).strftime("%Y-%m-%d")
    return {
        "as_of": as_of,
        "source": "금융통계정보시스템(FISIS) OpenAPI",
        "source_url": explicit_url or "http://fisis.fss.or.kr/openapi/statisticsInfoSearch.json",
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

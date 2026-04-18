"""
LH / 국토교통부 개발 공시 + 시중은행 점포 수 / 지역별 점포 현황
- 개발 공시: 구글뉴스 RSS 우회 (공식 API 없음)
- 점포 수: FISIS OpenAPI 4-step 체인 (companySearch → statisticsListSearch → accountListSearch → statisticsInfoSearch)
  · 국내은행(partDiv=A) / 일반현황(smlDiv=A) / 통계표 "영업점포현황" + "지역별 점포 현황"
  · 발견된 코드는 data/fisis_codes.json 에 캐싱
  · 실패 시 은행연합회 스크래핑 → branch_stats_manual.json 순서로 fallback
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

# ====== FISIS OpenAPI ======
FISIS_BASE = "http://fisis.fss.or.kr/openapi"
FISIS_PART_DIV_DOMESTIC_BANK = "A"   # 국내은행
FISIS_LRG_DIV_BANK = "A"
FISIS_SML_DIV_GENERAL = "A"           # 일반현황
FISIS_CACHE_FILE = DATA_DIR / "fisis_codes.json"
FISIS_LIST_KEYWORD_BRANCH = "영업점포현황"
FISIS_LIST_KEYWORD_REGIONAL = "지역별 점포"

KFB_BRANCH_URL_CANDIDATES = [
    "https://portal.kfb.or.kr/fingoods/saving/listBranchStatistics.do",
    "https://portal.kfb.or.kr/fingoods/saving/branchStatistics.do",
    "https://portal.kfb.or.kr/fingoods/branch/branchStatistics.do",
    "https://www.kfb.or.kr/consumer/stat/branch.php",
    "https://exchange.kfb.or.kr/page/branch.php",
]

TARGET_BANKS = [
    {"name": "KB국민", "aliases": ["kb국민은행", "kb국민", "국민은행", "kookmin"]},
    {"name": "신한", "aliases": ["신한은행", "신한", "shinhan"]},
    {"name": "하나", "aliases": ["하나은행", "keb하나", "하나", "hana"]},
    {"name": "우리", "aliases": ["우리은행", "우리", "woori"]},
    {"name": "NH농협", "aliases": ["nh농협은행", "농협은행", "nh농협", "농협", "nonghyup"]},
]

REGION_ORDER = [
    "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
    "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
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


# ---------- utils ----------
def make_id(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", "", raw)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _norm(s: str) -> str:
    return re.sub(r"\s+", "", (s or "").strip()).lower()


def _extract_int(s):
    if s is None:
        return None
    nums = re.findall(r"\d[\d,]*", str(s))
    if not nums:
        return None
    try:
        return int(nums[-1].replace(",", ""))
    except ValueError:
        return None


def _map_bank_name(raw_name: str):
    if not raw_name:
        return None
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


def _ym_to_asof(ym: str):
    if not re.fullmatch(r"\d{6}", ym or ""):
        return datetime.now(KST).strftime("%Y-%m-%d")
    mo = ym[4:6]
    day = "31" if mo in ("03", "05", "07", "08", "10", "12") else ("30" if mo in ("04", "06", "09", "11") else "28")
    return f"{ym[:4]}-{mo}-{day}"


# ---------- development news ----------
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

    seen = set()
    result = []
    for item in all_items:
        key = item["title"][:30]
        if key in seen:
            continue
        seen.add(key)
        result.append(item)

    cutoff = datetime.now(KST) - timedelta(days=7)
    result = [x for x in result if datetime.fromisoformat(x["published"]) >= cutoff]
    result.sort(key=lambda x: x["published"], reverse=True)
    return result[:15]


# ---------- FISIS OpenAPI ----------
# FISIS 문서 "예제URL"/"예제요청결과" 는 XML 포맷 기준이라, XML을 1차 응답으로 사용하고
# JSON은 보조 경로로 유지한다. XML은 BeautifulSoup(lxml/xml)로 파싱.
def _fisis_parse_xml(body: bytes):
    """bytes → BeautifulSoup. lxml 미설치 환경에서는 html.parser로 fallback."""
    try:
        return BeautifulSoup(body, "xml")  # lxml-xml
    except Exception:
        try:
            return BeautifulSoup(body, "lxml")
        except Exception:
            return BeautifulSoup(body, "html.parser")


def _fisis_log_sample(endpoint: str, text: str, limit: int = 400):
    sample = re.sub(r"\s+", " ", (text or "")).strip()[:limit]
    print(f"[fisis] {endpoint} sample: {sample}")


def _fisis_get_xml(endpoint: str, **params):
    """FISIS XML API GET → BeautifulSoup 또는 None."""
    api_key = os.environ.get("FISIS_API_KEY", "").strip()
    if not api_key:
        print(f"[fisis] {endpoint}: FISIS_API_KEY not set")
        return None
    params.setdefault("auth", api_key)
    params.setdefault("lang", "kr")
    url = f"{FISIS_BASE}/{endpoint}.xml"
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=25)
        if resp.status_code != 200:
            print(f"[fisis] {endpoint} http={resp.status_code}")
            _fisis_log_sample(endpoint, resp.text)
            return None
        soup = _fisis_parse_xml(resp.content)
        err_cd_tag = soup.find(re.compile(r"^err_cd$", re.I))
        if err_cd_tag:
            code = (err_cd_tag.get_text() or "").strip()
            if code and code != "000":
                err_msg_tag = soup.find(re.compile(r"^err_msg$", re.I))
                msg = err_msg_tag.get_text().strip() if err_msg_tag else ""
                print(f"[fisis] {endpoint} api_err={code} msg='{msg}'")
                return None
        return soup
    except Exception as e:
        print(f"[fisis] {endpoint} request error: {e}")
        return None


def _fisis_get_json(endpoint: str, **params):
    """FISIS JSON API GET (보조 경로) → dict 또는 None."""
    api_key = os.environ.get("FISIS_API_KEY", "").strip()
    if not api_key:
        return None
    params.setdefault("auth", api_key)
    params.setdefault("lang", "kr")
    url = f"{FISIS_BASE}/{endpoint}.json"
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=25)
        if resp.status_code != 200:
            return None
        try:
            return resp.json()
        except Exception:
            return None
    except Exception:
        return None


# XML row 태그 후보. FISIS 응답에서 반복 row는 아래 이름들 중 하나로 추정.
_FISIS_ROW_TAG_CANDIDATES = ("list", "row", "item", "record", "dataSet")


def _xml_row_dict(node) -> dict:
    """XML element를 {자식태그: 텍스트} dict로 변환."""
    d = {}
    for child in node.children:
        name = getattr(child, "name", None)
        if not name:
            continue
        txt = child.get_text(strip=True) if hasattr(child, "get_text") else ""
        if txt:
            d[name] = txt
    return d


def _fisis_extract_rows_any(resp):
    """XML(soup) / JSON(dict) 응답에서 row dict 리스트 추출."""
    if resp is None:
        return []
    # XML path
    if hasattr(resp, "find_all"):
        # err_cd만 있는 래퍼 제외
        for tag in _FISIS_ROW_TAG_CANDIDATES:
            nodes = [n for n in resp.find_all(tag) if n.find(re.compile(r"err_cd|err_msg", re.I)) is None]
            if not nodes:
                # err_cd 체크 없이 모든 태그 수집도 시도
                nodes = resp.find_all(tag)
            # 실질적으로 자식이 있는 것만
            nodes = [n for n in nodes if _xml_row_dict(n)]
            if nodes:
                return [_xml_row_dict(n) for n in nodes]
        # 최후의 수단: <result> 바로 밑에 있는 모든 element를 각각 row로 간주
        top = resp.find("result")
        if top:
            candidate_rows = []
            for child in top.find_all(recursive=False):
                if child.name in ("err_cd", "err_msg"):
                    continue
                d = _xml_row_dict(child)
                if d:
                    candidate_rows.append(d)
            if candidate_rows:
                return candidate_rows
        return []
    # JSON path (dict)
    if isinstance(resp, dict):
        for key in ["result", "list", "data", "items", "row"]:
            v = resp.get(key)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                for inner in ["list", "row", "data", "items"]:
                    vv = v.get(inner)
                    if isinstance(vv, list):
                        return vv
        for nk in ["response", "body"]:
            nested = resp.get(nk)
            if isinstance(nested, dict):
                for key in ["result", "list", "data", "items", "row"]:
                    v = nested.get(key)
                    if isinstance(v, list):
                        return v
    return []


def _fisis_call(endpoint: str, **params):
    """XML 우선, 실패 시 JSON fallback. row list 반환."""
    soup = _fisis_get_xml(endpoint, **params)
    rows = _fisis_extract_rows_any(soup)
    if rows:
        return rows
    data = _fisis_get_json(endpoint, **params)
    rows = _fisis_extract_rows_any(data)
    if not rows:
        print(f"[fisis] {endpoint} zero rows (xml+json). params={ {k:v for k,v in params.items() if k!='auth'} }")
    return rows


def _fisis_first(row: dict, keys):
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    # case-insensitive retry
    low = {k.lower(): v for k, v in row.items()}
    for k in keys:
        v = low.get(k.lower())
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _fisis_row_ym(row: dict):
    v = _fisis_first(row, ["baseYm", "basYm", "baseMm", "base_mm", "baseYymm", "period"])
    if re.fullmatch(r"\d{6}", v):
        return v
    txt = " ".join(str(x) for x in row.values())
    m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", txt)
    return "".join(m.groups()) if m else ""


def _fisis_row_value(row: dict):
    val = _fisis_first(row, ["dataValue", "value", "val", "resultVal", "amt", "cnt", "count"])
    n = _extract_int(val)
    if n is not None:
        return n
    for v in row.values():
        n = _extract_int(v)
        if n is not None:
            return n
    return None


def fisis_find_bank_finance_codes():
    """companySearch(partDiv=A) → 5대 은행 financeCd 매핑."""
    rows = _fisis_call("companySearch", partDiv=FISIS_PART_DIV_DOMESTIC_BANK)
    print(f"[fisis] companySearch returned {len(rows)} rows")
    result = {}
    for row in rows:
        name = _fisis_first(row, ["financeNm", "financenm", "companyNm", "finCompNm", "kor_co_nm", "finName", "name", "nm"])
        code = _fisis_first(row, ["financeCd", "financecd", "finCompCd", "finCd", "code", "cd"])
        if not name or not code:
            continue
        mapped = _map_bank_name(name)
        if mapped and mapped not in result:
            result[mapped] = code
            print(f"[fisis] financeCd discovered: {mapped} = {code} ({name})")
    missing = [b["name"] for b in TARGET_BANKS if b["name"] not in result]
    if missing:
        print(f"[fisis] financeCd unresolved for: {missing}")
    return result


def fisis_find_list_no(keywords):
    """statisticsListSearch(lrgDiv=A, smlDiv=A) → 키워드 집합 중 하나라도 매칭되는 listNo 반환.

    keywords: str 또는 str tuple. 각 키워드는 공백 제거·소문자 비교.
    """
    if isinstance(keywords, str):
        keywords = (keywords,)
    rows = _fisis_call("statisticsListSearch",
                       lrgDiv=FISIS_LRG_DIV_BANK,
                       smlDiv=FISIS_SML_DIV_GENERAL)
    print(f"[fisis] statisticsListSearch(A,A) returned {len(rows)} rows")
    norm_kws = [_norm(k) for k in keywords]
    first_names = []
    for row in rows:
        name = _fisis_first(row, ["listNm", "listname", "listName", "stsListNm", "name", "nm"])
        code = _fisis_first(row, ["listNo", "listno", "stsListNo", "code", "cd"])
        if not name or not code:
            continue
        if len(first_names) < 10:
            first_names.append(f"{code}:{name}")
        nn = _norm(name)
        for kw in norm_kws:
            if kw and kw in nn:
                print(f"[fisis] listNo discovered: '{keywords[0]}' → {code} ({name})")
                return code
    print(f"[fisis] listNo not found for keywords {keywords}. top-10 seen: {first_names}")
    return None


def fisis_load_cache():
    if not FISIS_CACHE_FILE.exists():
        return {}
    try:
        return json.loads(FISIS_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def fisis_save_cache(codes: dict):
    try:
        FISIS_CACHE_FILE.write_text(
            json.dumps(codes, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        print(f"[fisis] cache save fail: {e}")


def fisis_discover_codes():
    """발견 + 캐시. 환경변수로 수동 override 가능."""
    cached = fisis_load_cache()
    bank_cds = dict(cached.get("bank_finance_codes") or {})
    list_no_branch = cached.get("list_no_branch") or os.environ.get("FISIS_LIST_NO_BRANCH", "").strip()
    list_no_regional = cached.get("list_no_regional") or os.environ.get("FISIS_LIST_NO_REGIONAL", "").strip()

    if len(bank_cds) < 5:
        discovered = fisis_find_bank_finance_codes()
        bank_cds.update(discovered)
    if not list_no_branch:
        list_no_branch = fisis_find_list_no((FISIS_LIST_KEYWORD_BRANCH, "영업점포", "영업점 현황", "점포현황"))
    if not list_no_regional:
        list_no_regional = fisis_find_list_no((FISIS_LIST_KEYWORD_REGIONAL, "지역별점포", "지역별 점포", "지역별영업점"))

    codes = {
        "bank_finance_codes": bank_cds,
        "list_no_branch": list_no_branch or "",
        "list_no_regional": list_no_regional or "",
        "discovered_at": datetime.now(KST).isoformat(),
    }
    fisis_save_cache(codes)
    return codes


def _fisis_fetch_info(list_no: str, finance_cd: str, months_back: int = 18):
    """statisticsInfoSearch 호출. 최근 months_back 개월 범위의 분기 데이터."""
    now = datetime.now(KST)
    end_ym = now.strftime("%Y%m")
    start_dt = (now.replace(day=1) - timedelta(days=months_back * 31))
    start_ym = start_dt.strftime("%Y%m")
    return _fisis_call("statisticsInfoSearch",
                       financeCd=finance_cd,
                       listNo=list_no,
                       term="Q",
                       startBaseMm=start_ym,
                       endBaseMm=end_ym)


def fisis_build_branch_stats(codes: dict):
    """영업점포현황 → 은행별 {branches, sub_offices, count}."""
    list_no = codes.get("list_no_branch")
    bank_cds = codes.get("bank_finance_codes") or {}
    if not list_no or len(bank_cds) < 5:
        return None

    latest_ym_overall = ""
    per_bank = {}

    for meta in TARGET_BANKS:
        bank = meta["name"]
        finance_cd = bank_cds.get(bank)
        if not finance_cd:
            print(f"[fisis] missing financeCd: {bank}")
            continue
        rows = _fisis_fetch_info(list_no, finance_cd)
        if not rows:
            print(f"[fisis] no rows for {bank} (listNo={list_no})")
            continue

        yms = sorted({_fisis_row_ym(r) for r in rows} - {""}, reverse=True)
        if not yms:
            continue
        latest_ym = yms[0]
        if latest_ym > latest_ym_overall:
            latest_ym_overall = latest_ym
        latest_rows = [r for r in rows if _fisis_row_ym(r) == latest_ym]

        branches = None
        sub_offices = None
        sum_total = None
        for row in latest_rows:
            name = _fisis_first(row, ["accountNm", "acntNm", "acntName", "itemNm", "itemName", "name"])
            val = _fisis_row_value(row)
            if val is None:
                continue
            n = _norm(name)
            if "출장소" in n:
                sub_offices = (sub_offices or 0) + val if n != "출장소" else val
                if n == "출장소":
                    sub_offices = val
            elif any(k in n for k in ["지점", "영업점", "본점", "branch"]):
                if n in ("계", "소계", "합계"):
                    continue
                # 세부 구분(시/도점, 일반점 등)을 더하는 것보다 "지점계"/"합계" 우선
                if branches is None:
                    branches = val
                else:
                    branches += val
            elif n in ("점포수", "점포", "총계", "계", "합계"):
                sum_total = val

        # 보정: 지점·출장소가 없고 총계만 있으면 그걸 전체로
        if branches is None and sub_offices is None and sum_total is not None:
            branches = sum_total
            sub_offices = 0
        if branches is None:
            continue
        if sub_offices is None:
            sub_offices = 0

        per_bank[bank] = {
            "branches": int(branches),
            "sub_offices": int(sub_offices),
            "count": int(branches) + int(sub_offices),
        }
        time.sleep(0.3)

    if len(per_bank) < 5:
        return None

    banks = []
    for meta in TARGET_BANKS:
        rec = per_bank.get(meta["name"])
        if not rec:
            return None
        banks.append({"name": meta["name"], **rec})

    if not all(100 <= b["count"] <= 3000 for b in banks):
        print(f"[fisis] branch count out of range: {[b['count'] for b in banks]}")
        return None

    return {
        "as_of": _ym_to_asof(latest_ym_overall),
        "latest_ym": latest_ym_overall,
        "banks": banks,
    }


def fisis_build_regional_stats(codes: dict):
    """지역별 점포 현황 → [{region, banks:[{name,count}]}]."""
    list_no = codes.get("list_no_regional")
    bank_cds = codes.get("bank_finance_codes") or {}
    if not list_no or len(bank_cds) < 5:
        return None

    region_map = {}
    latest_ym_overall = ""

    for meta in TARGET_BANKS:
        bank = meta["name"]
        finance_cd = bank_cds.get(bank)
        if not finance_cd:
            continue
        rows = _fisis_fetch_info(list_no, finance_cd, months_back=9)
        if not rows:
            continue
        yms = sorted({_fisis_row_ym(r) for r in rows} - {""}, reverse=True)
        if not yms:
            continue
        latest_ym = yms[0]
        if latest_ym > latest_ym_overall:
            latest_ym_overall = latest_ym
        latest_rows = [r for r in rows if _fisis_row_ym(r) == latest_ym]

        for row in latest_rows:
            region = _fisis_first(row, [
                "region", "regionNm", "area", "areaNm",
                "siNm", "sidoNm", "zoneNm", "zoneName",
                "accountNm", "acntNm", "itemNm", "name",
            ])
            if not region:
                continue
            if region in ("합계", "소계", "총계", "계", "전국", "전 국", "total"):
                continue
            val = _fisis_row_value(row)
            if val is None:
                continue
            region_map.setdefault(region, {})[bank] = int(val)
        time.sleep(0.3)

    if not region_map:
        return None

    def sort_key(nm):
        for i, o in enumerate(REGION_ORDER):
            if o in nm:
                return i
        return 999

    regional = []
    for region, bank_dict in sorted(region_map.items(), key=lambda x: sort_key(x[0])):
        banks = [{"name": m["name"], "count": int(bank_dict.get(m["name"], 0))} for m in TARGET_BANKS]
        if sum(b["count"] for b in banks) == 0:
            continue
        regional.append({"region": region, "banks": banks})

    if not regional:
        return None

    return {"latest_ym": latest_ym_overall, "regional": regional}


def fetch_branch_stats_from_fisis():
    codes = fisis_discover_codes()
    branch = fisis_build_branch_stats(codes)
    if not branch:
        return None
    regional_data = fisis_build_regional_stats(codes)
    return {
        "as_of": branch["as_of"],
        "source": "금융통계정보시스템(FISIS) · 국내은행 영업점포현황",
        "source_url": f"{FISIS_BASE}/statisticsInfoSearch.json",
        "banks": branch["banks"],
        "regional": (regional_data or {}).get("regional", []),
        "is_fallback": False,
    }


# ---------- KFB scrape fallback ----------
def fetch_branch_stats_from_kfb():
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
                if not any(k in row_text for k in ["점포", "영업점", "지점", "은행"]):
                    continue
                bank = _map_bank_name(cells[0])
                if not bank:
                    for c in cells:
                        bank = _map_bank_name(c)
                        if bank:
                            break
                if not bank:
                    continue
                count = None
                for c in reversed(cells):
                    count = _extract_int(c)
                    if count is not None:
                        break
                if count is None:
                    continue
                parsed[bank] = count

            if len(parsed) >= 5:
                banks = []
                for b in TARGET_BANKS:
                    total = parsed[b["name"]]
                    banks.append({
                        "name": b["name"],
                        "branches": total,
                        "sub_offices": 0,
                        "count": total,
                    })
                if not all(100 <= x["count"] <= 3000 for x in banks):
                    continue
                if not (1000 <= sum(x["count"] for x in banks) <= 10000):
                    continue
                return {
                    "as_of": as_of or datetime.now(KST).strftime("%Y-%m-%d"),
                    "source": "은행연합회 소비자포털 점포 통계",
                    "source_url": url,
                    "banks": banks,
                    "regional": [],
                    "is_fallback": False,
                }
        except Exception as e:
            print(f"[stats] kfb scrape fail ({url}): {e}")
    return None


# ---------- orchestration ----------
def load_previous_stats():
    for p in [DATA_DIR / "branch_stats.json", DATA_DIR / "branch_stats_manual.json"]:
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
    return {"banks": []}


def load_branch_stats():
    """FISIS → KFB 스크래핑 → 수동 파일 순."""
    fisis = fetch_branch_stats_from_fisis()
    if fisis:
        return fisis

    scraped = fetch_branch_stats_from_kfb()
    if scraped:
        return scraped

    stat_file = DATA_DIR / "branch_stats_manual.json"
    if stat_file.exists():
        fallback = json.loads(stat_file.read_text(encoding="utf-8"))
        fallback["source"] = f"{fallback.get('source', '수동 입력')} (FISIS·스크래핑 실패로 fallback)"
        fallback["is_fallback"] = True
        for b in fallback.get("banks", []):
            if "branches" not in b:
                b["branches"] = b.get("count", 0)
                b["sub_offices"] = 0
            else:
                b.setdefault("sub_offices", 0)
                b["count"] = int(b.get("branches", 0)) + int(b.get("sub_offices", 0))
        fallback.setdefault("regional", [])
        return fallback

    return {
        "as_of": datetime.now(KST).strftime("%Y-%m-%d"),
        "source": "데이터 없음 (FISIS·스크래핑 모두 실패)",
        "banks": [
            {"name": b["name"], "branches": 0, "sub_offices": 0, "count": 0} for b in TARGET_BANKS
        ],
        "regional": [],
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
            "branches": int(b.get("branches", b["count"])),
            "sub_offices": int(b.get("sub_offices", 0)),
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
        "regional": stats.get("regional", []),
    }
    (DATA_DIR / "branch_stats.json").write_text(
        json.dumps(stats_output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[stats] saved: as_of={stats.get('as_of')} fallback={stats_output['is_fallback']} regional_n={len(stats_output['regional'])}")


if __name__ == "__main__":
    main()

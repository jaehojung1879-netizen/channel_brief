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
# 공식 도메인은 http만 운영. 일부 환경에서 https 강제 redirect 가 필요할 수 있어 둘 다 시도.
FISIS_BASE_CANDIDATES = [
    "http://fisis.fss.or.kr/openapi",
    "https://fisis.fss.or.kr/openapi",
]
FISIS_BASE = FISIS_BASE_CANDIDATES[0]
FISIS_PART_DIV_DOMESTIC_BANK = "A"   # 국내은행
FISIS_LRG_DIV_BANK = "A"
FISIS_SML_DIV_GENERAL = "A"           # 일반현황
FISIS_CACHE_FILE = DATA_DIR / "fisis_codes.json"
FISIS_LIST_KEYWORD_BRANCH = "영업점포현황"
FISIS_LIST_KEYWORD_REGIONAL = "지역별 점포"
FISIS_LIST_KEYWORD_ATM = "자동화기기 설치현황"
ATM_ACCOUNT_CODES = tuple("ABCDEFG")

KFB_BRANCH_URL_CANDIDATES = [
    "https://portal.kfb.or.kr/fingoods/saving/listBranchStatistics.do",
    "https://portal.kfb.or.kr/fingoods/saving/branchStatistics.do",
    "https://portal.kfb.or.kr/fingoods/branch/branchStatistics.do",
    "https://www.kfb.or.kr/consumer/stat/branch.php",
    "https://exchange.kfb.or.kr/page/branch.php",
]

TARGET_BANKS = [
    {"name": "신한", "aliases": ["신한은행", "신한", "shinhan"]},
    {"name": "KB국민", "aliases": ["kb국민은행", "kb국민", "국민은행", "kookmin"]},
    {"name": "하나", "aliases": ["하나은행", "keb하나", "하나", "hana"]},
    {"name": "우리", "aliases": ["우리은행", "우리", "woori"]},
]

REGION_ORDER = [
    "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
    "경기", "강원", "충청", "경상", "전라", "제주",
    "충북", "충남", "전북", "전남", "경북", "경남",
]

# FISIS "일반현황-지역별 점포 현황" 지역코드 매핑
REGION_CODE_MAP = {
    "A": "서울특별시",
    "B": "인천광역시",
    "C": "부산광역시",
    "D": "대구광역시",
    "E": "대전광역시",
    "F": "울산광역시",
    "G": "광주광역시",
    "H": "경기도",
    "I": "강원도",
    "J": "충청도",
    "K": "경상도",
    "L": "전라도",
    "M": "제주도",
    "N": "세종특별자치시",
    "O": "합계",
}

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


def _resolve_region_name(row: dict):
    """FISIS row에서 지역명/지역코드를 해석해 표준 지역명 반환."""
    code = _fisis_first(row, [
        "regionCd", "region_cd", "areaCd", "area_cd", "zoneCd", "zone_cd", "localCd", "local_cd",
        "accountCd", "account_cd", "acntCd", "acnt_cd",
    ])
    if code:
        code = str(code).strip().upper()
        if code in REGION_CODE_MAP:
            return REGION_CODE_MAP[code]
    return _fisis_first(row, [
        "region", "regionNm", "area", "areaNm",
        "siNm", "sidoNm", "sigunguNm", "guNm",
        "zoneNm", "zoneName", "localNm", "local_nm",
        "accountNm", "acntNm", "itemNm", "name",
    ])


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


def _mask_key(k: str) -> str:
    if not k:
        return "(empty)"
    if len(k) <= 6:
        return "***"
    return f"{k[:3]}***{k[-3:]} (len={len(k)})"


def _fisis_get_xml(endpoint: str, **params):
    """FISIS XML API GET → BeautifulSoup 또는 None. http→https 순차 시도."""
    api_key = os.environ.get("FISIS_API_KEY", "").strip()
    if not api_key:
        print(f"[fisis] {endpoint}: FISIS_API_KEY not set (env=missing)")
        return None
    params.setdefault("auth", api_key)
    params.setdefault("lang", "kr")
    last_err = None
    for base in FISIS_BASE_CANDIDATES:
        url = f"{base}/{endpoint}.xml"
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=25, allow_redirects=True)
            print(f"[fisis] GET {url} http={resp.status_code} bytes={len(resp.content)} key={_mask_key(api_key)}")
            if resp.status_code != 200:
                _fisis_log_sample(endpoint, resp.text)
                last_err = f"http={resp.status_code}"
                continue
            soup = _fisis_parse_xml(resp.content)
            err_cd_tag = soup.find(re.compile(r"^err_cd$", re.I))
            if err_cd_tag:
                code = (err_cd_tag.get_text() or "").strip()
                if code and code != "000":
                    err_msg_tag = soup.find(re.compile(r"^err_msg$", re.I))
                    msg = err_msg_tag.get_text().strip() if err_msg_tag else ""
                    print(f"[fisis] {endpoint} api_err={code} msg='{msg}'")
                    _fisis_log_sample(endpoint, resp.text)
                    return None
            return soup
        except Exception as e:
            last_err = str(e)
            print(f"[fisis] {endpoint} request error on {base}: {e}")
            continue
    print(f"[fisis] {endpoint} all bases failed (last={last_err})")
    return None


def _fisis_get_json(endpoint: str, **params):
    """FISIS JSON API GET (보조 경로) → dict 또는 None."""
    api_key = os.environ.get("FISIS_API_KEY", "").strip()
    if not api_key:
        return None
    params.setdefault("auth", api_key)
    params.setdefault("lang", "kr")
    for base in FISIS_BASE_CANDIDATES:
        url = f"{base}/{endpoint}.json"
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=25, allow_redirects=True)
            print(f"[fisis] GET {url} http={resp.status_code} bytes={len(resp.content)} (json)")
            if resp.status_code != 200:
                continue
            try:
                return resp.json()
            except Exception:
                _fisis_log_sample(endpoint + " json", resp.text)
                continue
        except Exception as e:
            print(f"[fisis] {endpoint} json error on {base}: {e}")
            continue
    return None


def _xml_children_tag_counts(node):
    """direct children 태그별 빈도수."""
    counts = {}
    for c in node.find_all(recursive=False):
        name = getattr(c, "name", None)
        if name:
            counts[name] = counts.get(name, 0) + 1
    return counts


def _is_wrapper(node):
    """같은 태그 자식이 2개 이상이면 wrapper (<list><row>…</row><row>…</row></list>)."""
    counts = _xml_children_tag_counts(node)
    return any(v >= 2 for v in counts.values())


def _xml_row_dict(node) -> dict:
    """XML leaf element → {자식태그: 텍스트}. wrapper 노드는 {} 반환."""
    if _is_wrapper(node):
        return {}
    d = {}
    for child in node.children:
        name = getattr(child, "name", None)
        if not name:
            continue
        txt = child.get_text(strip=True) if hasattr(child, "get_text") else ""
        if txt:
            d[name] = txt
    return d


def _drill_rows_from_wrapper(wrapper):
    """wrapper에서 반복되는 자식 태그를 찾아 row dict 리스트로 변환."""
    counts = _xml_children_tag_counts(wrapper)
    for bad in ("err_cd", "err_msg", "header"):
        counts.pop(bad, None)
    if not counts:
        return []
    top_tag = max(counts, key=counts.get)
    children = wrapper.find_all(top_tag, recursive=False)
    rows = [_xml_row_dict(c) for c in children]
    return [r for r in rows if r]


def _fisis_extract_rows_any(resp):
    """XML(soup) / JSON(dict) 응답에서 row dict 리스트 추출."""
    if resp is None:
        return []
    # XML path
    if hasattr(resp, "find_all"):
        # 1) row/item/record/dataSet 리프 태그 우선 탐색
        for tag in ("row", "item", "record", "dataSet"):
            nodes = resp.find_all(tag)
            dicts = [_xml_row_dict(n) for n in nodes]
            dicts = [d for d in dicts if d]
            if dicts:
                return dicts
        # 2) <list> 래퍼 안에서 반복 자식을 row로 추출
        for lst in resp.find_all("list"):
            rows = _drill_rows_from_wrapper(lst)
            if rows:
                return rows
        # 3) <result> 바로 밑 반복 자식
        top = resp.find("result")
        if top:
            rows = _drill_rows_from_wrapper(top)
            if rows:
                return rows
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
    v = _fisis_first(row, [
        "baseYm", "base_ym", "basYm", "bas_ym",
        "baseMm", "base_mm", "baseYymm", "base_yymm",
        "period", "stat_ym", "stnd_ym",
    ])
    if re.fullmatch(r"\d{6}", v):
        return v
    txt = " ".join(str(x) for x in row.values())
    m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", txt)
    return "".join(m.groups()) if m else ""


def _fisis_row_value(row: dict):
    val = _fisis_first(row, [
        "a", "b", "c", "d",
        "dataValue", "data_value", "value", "val", "resultVal", "result_val",
        "amt", "cnt", "count", "num", "qty",
    ])
    n = _extract_int(val)
    if n is not None:
        return n
    for k, v in row.items():
        if k in ("base_month", "baseMm", "baseYm", "finance_cd", "financeCd", "account_cd", "accountCd"):
            continue
        n = _extract_int(v)
        if n is not None:
            return n
    return None


def fisis_find_bank_finance_codes():
    """companySearch(partDiv=A) → 대상 은행 financeCd 매핑."""
    rows = _fisis_call("companySearch", partDiv=FISIS_PART_DIV_DOMESTIC_BANK)
    print(f"[fisis] companySearch returned {len(rows)} rows")
    if rows:
        print(f"[fisis] companySearch row[0] keys: {list(rows[0].keys())}")
        print(f"[fisis] companySearch row[0] data: {rows[0]}")
        if len(rows) > 1:
            print(f"[fisis] companySearch row[1] data: {rows[1]}")
    result = {}
    for row in rows:
        # 알려진 키로 먼저 시도, 실패 시 값 패턴(한글 은행명/숫자 코드)으로 fallback
        name = _fisis_first(row, [
            "financeNm", "financenm", "fin_nm",
            "companyNm", "companynm", "finCompNm", "fin_co_nm",
            "kor_co_nm", "finName", "name", "nm",
        ])
        code = _fisis_first(row, [
            "financeCd", "financecd", "fin_cd",
            "finCompCd", "fin_co_cd", "finCd", "code", "cd",
        ])
        if not name:
            for v in row.values():
                if isinstance(v, str) and re.search(r"은행|Bank", v):
                    name = v
                    break
        if not code:
            for k, v in row.items():
                if isinstance(v, str) and re.fullmatch(r"\d{4,}", v.strip()) and "cd" in k.lower():
                    code = v.strip()
                    break
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
    if rows:
        print(f"[fisis] statisticsListSearch row[0] keys: {list(rows[0].keys())}")
        print(f"[fisis] statisticsListSearch row[0] data: {rows[0]}")
    norm_kws = [_norm(k) for k in keywords]
    first_names = []
    for row in rows:
        name = _fisis_first(row, [
            "listNm", "listnm", "list_nm",
            "listname", "listName", "stsListNm", "sts_list_nm",
            "name", "nm", "title",
        ])
        code = _fisis_first(row, [
            "listNo", "listno", "list_no",
            "stsListNo", "sts_list_no", "code", "cd",
        ])
        # fallback: any value containing target keywords → take the whole row
        if not name:
            for v in row.values():
                if isinstance(v, str) and ("점포" in v or "영업점" in v):
                    name = v
                    break
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


def _looks_like_regional_rows(rows: list) -> bool:
    """지역별 통계표 응답인지 휴리스틱 판별."""
    if not rows:
        return False
    hit = 0
    for r in rows[:60]:
        region = _resolve_region_name(r)
        if region and region not in ("합계", "소계", "총계", "계", "전국", "전 국", "total"):
            hit += 1
    return hit >= 3


def fisis_find_regional_list_no_by_probe(bank_cds: dict):
    """listNo 자동 발견 실패 시, 후보 listNo를 실제 조회해 지역코드/지역명 포함 응답인지 검사."""
    if not bank_cds:
        return None
    probe_fin_cd = next(iter(bank_cds.values()), "")
    if not probe_fin_cd:
        return None
    rows = _fisis_call("statisticsListSearch", partDiv=FISIS_PART_DIV_DOMESTIC_BANK, lrgDiv=FISIS_LRG_DIV_BANK, smlDiv=FISIS_SML_DIV_GENERAL)
    if not rows:
        return None

    candidates = []
    for row in rows:
        name = _fisis_first(row, ["listNm", "listnm", "list_nm", "name", "title"]) or ""
        code = _fisis_first(row, ["listNo", "listno", "list_no", "stsListNo", "sts_list_no", "code", "cd"])
        if not code:
            continue
        nn = _norm(name)
        if "점포" in nn or "영업점" in nn:
            candidates.append((str(code), str(name)))
    # 이름이 더 직접적인 후보 우선
    candidates.sort(key=lambda x: (0 if ("지역별" in x[1] or "권역별" in x[1]) else 1, x[0]))
    for list_no, name in candidates[:30]:
        probe_rows = _fisis_fetch_info(list_no, probe_fin_cd, months_back=18)
        if _looks_like_regional_rows(probe_rows):
            print(f"[fisis] regional listNo probe hit: {list_no} ({name})")
            return list_no
    print("[fisis] regional listNo probe failed")
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
    list_no_branch = (
        cached.get("list_no_branch")
        or os.environ.get("FISIS_LIST_NO_BRANCH", "").strip()
        or os.environ.get("FISIS_LIST_NO", "").strip()  # legacy env
    )
    list_no_regional = (
        cached.get("list_no_regional")
        or os.environ.get("FISIS_LIST_NO_REGIONAL", "").strip()
        or os.environ.get("FISIS_LIST_NO", "").strip()  # legacy env
    )
    list_no_atm = (
        cached.get("list_no_atm")
        or os.environ.get("FISIS_LIST_NO_ATM", "").strip()
    )

    if len(bank_cds) < len(TARGET_BANKS):
        discovered = fisis_find_bank_finance_codes()
        bank_cds.update(discovered)
    if not list_no_branch:
        list_no_branch = fisis_find_list_no((FISIS_LIST_KEYWORD_BRANCH, "영업점포", "영업점 현황", "점포현황"))
    if not list_no_regional:
        list_no_regional = fisis_find_list_no((FISIS_LIST_KEYWORD_REGIONAL, "지역별점포", "지역별 점포", "지역별영업점"))
    if not list_no_regional:
        list_no_regional = fisis_find_regional_list_no_by_probe(bank_cds)
    if not list_no_atm:
        list_no_atm = fisis_find_list_no((FISIS_LIST_KEYWORD_ATM, "자동화기기설치현황", "자동화기기", "CD/ATM"))

    codes = {
        "bank_finance_codes": bank_cds,
        "list_no_branch": list_no_branch or "",
        "list_no_regional": list_no_regional or "",
        "list_no_atm": list_no_atm or "",
        "discovered_at": datetime.now(KST).isoformat(),
    }
    fisis_save_cache(codes)
    return codes


def _ym_add_months(ym: str, delta_months: int) -> str:
    if not ym or not re.fullmatch(r"\d{6}", ym):
        return ""
    y = int(ym[:4])
    m = int(ym[4:6])
    serial = y * 12 + (m - 1) + delta_months
    ny = serial // 12
    nm = (serial % 12) + 1
    return f"{ny:04d}{nm:02d}"


def _half_year_yms(latest_ym: str, years: int = 5) -> list:
    """latest_ym 기준, 6개월 간격 최근 N년 시계열. 예: 202606 기준 5년 → [202106, 202112, ..., 202606]."""
    if not latest_ym or not re.fullmatch(r"\d{6}", latest_ym):
        return []
    first = _ym_add_months(latest_ym, -(years * 12))
    if not first:
        return []
    yms = []
    cur = first
    while cur <= latest_ym:
        yms.append(cur)
        cur = _ym_add_months(cur, 6)
    return yms


def _fisis_fetch_info(list_no: str, finance_cd: str = "", months_back: int = 72, account_cd: str = ""):
    """statisticsInfoSearch 호출. 최근 months_back 개월 범위 데이터(분기 우선, 월 fallback)."""
    now = datetime.now(KST)
    end_ym = now.strftime("%Y%m")
    start_dt = (now.replace(day=1) - timedelta(days=months_back * 31))
    start_ym = start_dt.strftime("%Y%m")
    params = {
        "financeCd": finance_cd,
        "listNo": list_no,
        "startBaseMm": start_ym,
        "endBaseMm": end_ym,
    }
    if account_cd:
        params["accountCd"] = account_cd
    rows = _fisis_call("statisticsInfoSearch", term="Q", **params)
    if rows:
        return rows
    # 일부 통계표는 월 단위(term=M)만 응답
    return _fisis_call("statisticsInfoSearch", term="M", **params)


def _row_bucket_value(row: dict, prefer_cd: str = "A1") -> tuple:
    """단일 row에서 (account_cd, value)를 뽑아줌. 총계(A1) 우선."""
    cd = _fisis_first(row, ["account_cd", "accountCd", "acntCd", "acnt_cd"])
    val = _fisis_row_value(row)
    return cd, val


def _extract_branch_numbers(rows: list) -> dict:
    """주어진 row 리스트(한 시점/한 은행)에서 branches/sub_offices/count를 뽑음."""
    branches = None
    sub_offices = None
    sum_total = None
    by_cd = {}
    for row in rows:
        cd = _fisis_first(row, ["account_cd", "accountCd", "acntCd", "acnt_cd"])
        val = _fisis_row_value(row)
        if cd and val is not None:
            by_cd[cd] = val
    if "A11" in by_cd:
        branches = by_cd["A11"]
    if "A12" in by_cd:
        sub_offices = by_cd["A12"]
    if "A1" in by_cd:
        sum_total = by_cd["A1"]

    if branches is None or sub_offices is None:
        for row in rows:
            name = _fisis_first(row, [
                "accountNm", "account_nm", "acntNm", "acnt_nm",
                "acntName", "itemNm", "item_nm", "itemName", "name",
            ])
            val = _fisis_row_value(row)
            if val is None:
                continue
            n = _norm(name)
            if n in ("국내_지점", "지점", "국내지점"):
                if branches is None:
                    branches = val
            elif n in ("국내_출장소", "출장소", "국내출장소"):
                if sub_offices is None:
                    sub_offices = val
            elif n in ("국내", "점포수", "점포", "총계", "계", "합계"):
                if sum_total is None:
                    sum_total = val

    # FISIS XML may use Korean element names directly as field keys.
    # Also check wide-format single-row responses where
    # "a"=지점, "b"=출장소 인 경우가 존재.
    if branches is None:
        for row in rows:
            v = _extract_int(
                row.get("지점") or row.get("branch") or row.get("branchCnt") or row.get("branch_cnt")
                or row.get("a") or row.get("A")
            )
            if v is not None:
                branches = v
                break
    if sub_offices is None:
        for row in rows:
            v = _extract_int(
                row.get("출장소") or row.get("subOffice") or row.get("sub_office")
                or row.get("subOfficeCnt") or row.get("sub_office_cnt") or row.get("b") or row.get("B")
            )
            if v is not None:
                sub_offices = v
                break

    # wide-format에서 총계 필드가 없고 a/b만 내려오는 경우 합계를 보정
    if sum_total is None and branches is not None and sub_offices is not None:
        sum_total = branches + sub_offices

    if branches is not None and sub_offices is None and sum_total is not None:
        delta = sum_total - branches
        if 0 <= delta <= 2000:
            sub_offices = delta
    if branches is None and sub_offices is None and sum_total is not None:
        branches = sum_total
        sub_offices = 0
    if branches is None and sub_offices is None:
        return {}
    if branches is None:
        branches = 0
    if sub_offices is None:
        sub_offices = 0
    return {
        "branches": int(branches),
        "sub_offices": int(sub_offices),
        "count": int(branches) + int(sub_offices),
    }


def _normalize_branch_record(rec: dict) -> dict:
    count = int((rec or {}).get("count", 0) or 0)
    branches = int((rec or {}).get("branches", 0) or 0)
    sub_offices = int((rec or {}).get("sub_offices", 0) or 0)

    if branches > 0 and sub_offices > 0:
        # 둘 다 명확히 수집된 경우: 합산을 count로
        count = branches + sub_offices
    elif branches > 0 and sub_offices == 0 and count > branches:
        # 지점만 수집됐고 total > branches → 차이를 출장소로 추론
        sub_offices = count - branches
    elif sub_offices > 0 and branches == 0:
        branches = max(count - sub_offices, 0)
        count = branches + sub_offices
    elif branches > 0 and sub_offices == 0 and count == 0:
        count = branches
    elif count > 0 and branches == 0 and sub_offices == 0:
        # total만 있고 분해값 없음 → 일단 지점으로 간주
        branches = count
        sub_offices = 0

    return {"count": count, "branches": branches, "sub_offices": sub_offices}


def fisis_build_branch_stats(codes: dict):
    """영업점포현황 → 은행별 최신값 + 최신 기준월 anchor 5년(6개월 간격) 시계열."""
    list_no = codes.get("list_no_branch")
    bank_cds = codes.get("bank_finance_codes") or {}
    if not list_no or len(bank_cds) < len(TARGET_BANKS):
        return None

    latest_ym_overall = ""
    per_bank = {}
    per_bank_history = {}  # bank → {ym: count}

    for meta in TARGET_BANKS:
        bank = meta["name"]
        finance_cd = bank_cds.get(bank)
        if not finance_cd:
            print(f"[fisis] missing financeCd: {bank}")
            continue
        # 최근 6년치(=72개월) 범위로 확장하여 연말 데이터 5개 이상 확보
        rows = _fisis_fetch_info(list_no, finance_cd, months_back=72)
        print(f"[fisis] statisticsInfoSearch({bank},{finance_cd}) → {len(rows)} rows")
        if not rows:
            print(f"[fisis] no rows for {bank} (listNo={list_no})")
            continue

        yms = sorted({_fisis_row_ym(r) for r in rows} - {""}, reverse=True)
        if not yms:
            print(f"[fisis]   {bank} no parseable YM in rows. sample: {rows[0]}")
            continue
        latest_ym = yms[0]
        if latest_ym > latest_ym_overall:
            latest_ym_overall = latest_ym

        # 최신 시점 값
        latest_rows = [r for r in rows if _fisis_row_ym(r) == latest_ym]
        latest_vals = _extract_branch_numbers(latest_rows)
        if not latest_vals:
            continue
        per_bank[bank] = latest_vals

        # 최신 시점 기준 최근 5년(6개월 간격) 시계열
        history = {}
        target_yms = set(_half_year_yms(latest_ym, years=5))
        for ym in yms:
            if ym not in target_yms:
                continue
            period_rows = [r for r in rows if _fisis_row_ym(r) == ym]
            vals = _extract_branch_numbers(period_rows)
            if vals:
                history[ym] = vals["count"]
        per_bank_history[bank] = history
        print(f"[fisis]   {bank} latest={latest_vals['count']} history_points={len(history)}")
        time.sleep(0.3)

    if len(per_bank) < len(TARGET_BANKS):
        return None

    banks = []
    for meta in TARGET_BANKS:
        rec = per_bank.get(meta["name"])
        if not rec:
            return None
        hist = per_bank_history.get(meta["name"], {})
        banks.append({
            "name": meta["name"],
            **rec,
            "history": [{"ym": ym, "count": hist[ym]} for ym in sorted(hist.keys())],
        })

    if not all(100 <= b["count"] <= 3000 for b in banks):
        print(f"[fisis] branch count out of range: {[b['count'] for b in banks]}")
        return None

    return {
        "as_of": _ym_to_asof(latest_ym_overall),
        "latest_ym": latest_ym_overall,
        "banks": banks,
    }


def fisis_build_regional_stats(codes: dict):
    """지역별 점포 현황 → [{region, banks, history:[{ym, banks:[{name,count,branches,sub_offices}]}]}]."""
    list_no = codes.get("list_no_regional")
    bank_cds = codes.get("bank_finance_codes") or {}
    if not list_no or len(bank_cds) < len(TARGET_BANKS):
        return None

    # region → {ym → {bank → {count, branches, sub_offices}}}
    region_ym_bank = {}
    latest_ym_overall = ""

    def _merge_regional_rows(rows: list, bank: str, target_yms: set, kind: str):
        grouped_rows = {}
        for row in rows:
            ym = _fisis_row_ym(row)
            region = _resolve_region_name(row)
            if ym not in target_yms or not region:
                continue
            if region in ("합계", "소계", "총계", "계", "전국", "전 국", "total", REGION_CODE_MAP.get("O")):
                continue
            grouped_rows.setdefault((region, ym), []).append(row)

        for (region, ym), chunk in grouped_rows.items():
            rec = region_ym_bank.setdefault(region, {}).setdefault(ym, {}).setdefault(
                bank,
                {"count": 0, "branches": 0, "sub_offices": 0},
            )
            vals = _extract_branch_numbers(chunk)
            if vals:
                if kind == "branches":
                    rec["branches"] = max(rec["branches"], int(vals.get("branches", 0)))
                elif kind == "sub_offices":
                    rec["sub_offices"] = max(rec["sub_offices"], int(vals.get("sub_offices", 0)))
                else:  # kind == "count"
                    rec["count"] = max(rec["count"], int(vals.get("count", 0)))
                    b_val = int(vals.get("branches", 0))
                    s_val = int(vals.get("sub_offices", 0))
                    if b_val > 0:
                        rec["branches"] = max(rec["branches"], b_val)
                    if s_val > 0:
                        rec["sub_offices"] = max(rec["sub_offices"], s_val)
                continue
            fallback_val = max((_fisis_row_value(r) or 0) for r in chunk)
            if kind == "branches":
                rec["branches"] = max(rec["branches"], int(fallback_val))
            elif kind == "sub_offices":
                rec["sub_offices"] = max(rec["sub_offices"], int(fallback_val))
            else:
                rec["count"] = max(rec["count"], int(fallback_val))

    for meta in TARGET_BANKS:
        bank = meta["name"]
        finance_cd = bank_cds.get(bank)
        if not finance_cd:
            continue
        rows = _fisis_fetch_info(list_no, finance_cd, months_back=72)
        rows_branches = _fisis_fetch_info(list_no, finance_cd, months_back=72, account_cd="A11")
        rows_sub_offices = _fisis_fetch_info(list_no, finance_cd, months_back=72, account_cd="A12")
        if rows and bank == "신한":
            sample = rows[0]
            print(f"[fisis][regional] sample row keys: {list(sample.keys())}")
            print(f"[fisis][regional] sample row: {sample}")
            print(f"[fisis][regional] rows_sub_offices count: {len(rows_sub_offices)}")
        if not rows:
            continue
        yms = sorted({_fisis_row_ym(r) for r in rows} - {""}, reverse=True)
        if not yms:
            continue
        latest_ym = yms[0]
        if latest_ym > latest_ym_overall:
            latest_ym_overall = latest_ym
        # 최신 시점 기준 최근 5년(6개월 간격) + 최신
        target_yms = set(_half_year_yms(latest_ym, years=5)) | {latest_ym}

        _merge_regional_rows(rows, bank, target_yms, kind="count")
        if rows_branches:
            _merge_regional_rows(rows_branches, bank, target_yms, kind="branches")
        if rows_sub_offices:
            _merge_regional_rows(rows_sub_offices, bank, target_yms, kind="sub_offices")
        else:
            # Wide-format fallback: FISIS regional rows often have 지점 in field "a"
            # and 출장소 in field "b" of the SAME row (not separate A12 rows).
            # accountCd="A12" filter matches no region codes → rows_sub_offices empty.
            for row in rows:
                ym = _fisis_row_ym(row)
                region = _resolve_region_name(row)
                if ym not in target_yms or not region:
                    continue
                if region in ("합계", "소계", "총계", "계", "전국", "전 국", "total", REGION_CODE_MAP.get("O")):
                    continue
                b_val = _extract_int(
                    row.get("b") or row.get("B")
                    or row.get("출장소") or row.get("subOffice") or row.get("sub_office")
                    or row.get("subOfficeCnt") or row.get("sub_office_cnt")
                )
                if b_val is None:
                    continue
                rec = region_ym_bank.get(region, {}).get(ym, {}).get(bank)
                if rec is not None and b_val > 0:
                    rec["sub_offices"] = max(rec["sub_offices"], b_val)
                    rec["count"] = max(rec["count"], rec["branches"] + b_val)
        time.sleep(0.3)

    if not region_ym_bank:
        # 통계표 구조에 따라 financeCd 없이 전체 은행이 내려오는 경우 fallback 파싱
        rows = _fisis_fetch_info(list_no, finance_cd="", months_back=72)
        rows_branches = _fisis_fetch_info(list_no, finance_cd="", months_back=72, account_cd="A11")
        rows_sub_offices = _fisis_fetch_info(list_no, finance_cd="", months_back=72, account_cd="A12")

    if not region_ym_bank:
        # 통계표 구조에 따라 financeCd 없이 전체 은행이 내려오는 경우 fallback 파싱
        rows = _fisis_fetch_info(list_no, finance_cd="", months_back=72)
        rows_branches = _fisis_fetch_info(list_no, finance_cd="", months_back=72, account_cd="A11")
        rows_sub_offices = _fisis_fetch_info(list_no, finance_cd="", months_back=72, account_cd="A12")

        grouped_rows = {}
        for row in rows:
            ym = _fisis_row_ym(row)
            if not ym:
                continue
            if ym > latest_ym_overall:
                latest_ym_overall = ym
            bank_nm_raw = _fisis_first(row, ["financeNm", "finance_nm", "companyNm", "cmpyNm", "bankNm", "kor_co_nm", "name"])
            bank = _map_bank_name(bank_nm_raw)
            if not bank:
                continue
            region = _resolve_region_name(row)
            if not region or region in ("합계", "소계", "총계", "계", "전국", "전 국", "total", REGION_CODE_MAP.get("O")):
                continue
            val = _fisis_row_value(row)
            if val is None:
                continue
            # Initialize with count only; branches/sub_offices filled by subsequent loops
            rec = {"count": int(val), "branches": 0, "sub_offices": 0}
            region_ym_bank.setdefault(region, {}).setdefault(ym, {})[bank] = rec

        for row in rows_branches:
            ym = _fisis_row_ym(row)
            if not ym:
                continue
            bank = _map_bank_name(_fisis_first(row, ["financeNm", "finance_nm", "companyNm", "cmpyNm", "bankNm", "kor_co_nm", "name"]))
            region = _resolve_region_name(row)
            val = _fisis_row_value(row)
            if not bank or not region or val is None:
                continue
            if region in ("합계", "소계", "총계", "계", "전국", "전 국", "total", REGION_CODE_MAP.get("O")):
                continue
            rec = region_ym_bank.setdefault(region, {}).setdefault(ym, {}).setdefault(bank, {"count": 0, "branches": 0, "sub_offices": 0})
            rec["branches"] = max(rec["branches"], int(val))

        for row in rows_sub_offices:
            ym = _fisis_row_ym(row)
            if not ym:
                continue
            bank = _map_bank_name(_fisis_first(row, ["financeNm", "finance_nm", "companyNm", "cmpyNm", "bankNm", "kor_co_nm", "name"]))
            region = _resolve_region_name(row)
            val = _fisis_row_value(row)
            if not bank or not region or val is None:
                continue
            if region in ("합계", "소계", "총계", "계", "전국", "전 국", "total", REGION_CODE_MAP.get("O")):
                continue
            rec = region_ym_bank.setdefault(region, {}).setdefault(ym, {}).setdefault(bank, {"count": 0, "branches": 0, "sub_offices": 0})
            rec["sub_offices"] = max(rec["sub_offices"], int(val))

        if not rows_sub_offices:
            for row in rows:
                ym = _fisis_row_ym(row)
                bank_nm_raw = _fisis_first(row, ["financeNm", "finance_nm", "companyNm", "cmpyNm", "bankNm", "kor_co_nm", "name"])
                bank = _map_bank_name(bank_nm_raw)
                region = _resolve_region_name(row)
                if not ym or not bank or not region:
                    continue
                if region in ("합계", "소계", "총계", "계", "전국", "전 국", "total", REGION_CODE_MAP.get("O")):
                    continue
                b_val = _extract_int(
                    row.get("b") or row.get("B")
                    or row.get("출장소") or row.get("subOffice") or row.get("sub_office")
                    or row.get("subOfficeCnt") or row.get("sub_office_cnt")
                )
                if b_val is None or b_val <= 0:
                    continue
                rec = region_ym_bank.get(region, {}).get(ym, {}).get(bank)
                if rec is not None:
                    rec["sub_offices"] = max(rec["sub_offices"], b_val)

    if not region_ym_bank:
        return None

    def sort_key(nm):
        for i, o in enumerate(REGION_ORDER):
            if o in nm:
                return i
        return 999

    regional = []
    for region, ym_map in sorted(region_ym_bank.items(), key=lambda x: sort_key(x[0])):
        latest = ym_map.get(latest_ym_overall, {})
        latest_banks = []
        for m in TARGET_BANKS:
            rec = _normalize_branch_record(latest.get(m["name"], {}) or {})
            latest_banks.append({
                "name": m["name"],
                "count": int(rec["count"]),
                "branches": int(rec["branches"]),
                "sub_offices": int(rec["sub_offices"]),
            })
        if sum(b["count"] for b in latest_banks) == 0:
            continue
        history = []
        for ym in sorted(k for k in ym_map.keys()):
            per = ym_map[ym]
            hist_banks = []
            for m in TARGET_BANKS:
                rec = _normalize_branch_record(per.get(m["name"], {}) or {})
                hist_banks.append({
                    "name": m["name"],
                    "count": int(rec["count"]),
                    "branches": int(rec["branches"]),
                    "sub_offices": int(rec["sub_offices"]),
                })
            history.append({
                "ym": ym,
                "banks": hist_banks,
            })
        regional.append({"region": region, "banks": latest_banks, "history": history})

    if not regional:
        return None

    return {"latest_ym": latest_ym_overall, "regional": regional}


def fisis_fetch_account_labels(list_no: str) -> dict:
    """accountListSearch(listNo=...) → {accountCd: accountNm}."""
    if not list_no:
        return {}
    rows = _fisis_call("accountListSearch", listNo=list_no)
    labels = {}
    for row in rows:
        cd = _fisis_first(row, ["accountCd", "account_cd", "acntCd", "acnt_cd", "code", "cd"]).upper()
        nm = _fisis_first(row, ["accountNm", "account_nm", "acntNm", "acnt_nm", "name", "nm", "itemNm"])
        if cd:
            labels[cd] = nm or cd
    return labels


def fisis_build_atm_stats(codes: dict):
    """자동화기기 설치현황(listNo)에서 accountCd A~G 항목을 은행별로 수집."""
    list_no = codes.get("list_no_atm")
    bank_cds = codes.get("bank_finance_codes") or {}
    if not list_no or len(bank_cds) < len(TARGET_BANKS):
        return None

    labels = fisis_fetch_account_labels(list_no)
    latest_ym_overall = ""
    banks = []

    for meta in TARGET_BANKS:
        bank = meta["name"]
        finance_cd = bank_cds.get(bank)
        if not finance_cd:
            continue
        rows = _fisis_fetch_info(list_no, finance_cd, months_back=72)
        if not rows:
            continue
        yms = sorted({_fisis_row_ym(r) for r in rows} - {""}, reverse=True)
        if not yms:
            continue
        latest_ym = yms[0]
        latest_ym_overall = max(latest_ym_overall, latest_ym)

        latest_rows = [r for r in rows if _fisis_row_ym(r) == latest_ym]
        by_code = {code: 0 for code in ATM_ACCOUNT_CODES}
        for row in latest_rows:
            code = _fisis_first(row, ["accountCd", "account_cd", "acntCd", "acnt_cd", "code", "cd"]).upper()
            if code not in by_code:
                continue
            val = _fisis_row_value(row)
            if val is None:
                continue
            by_code[code] = max(by_code[code], int(val))

        items = [{
            "code": code,
            "name": labels.get(code) or f"코드 {code}",
            "count": int(by_code.get(code, 0)),
        } for code in ATM_ACCOUNT_CODES]
        total = sum(x["count"] for x in items)
        banks.append({"name": bank, "items": items, "total": total})
        time.sleep(0.25)

    if len(banks) < len(TARGET_BANKS):
        return None

    return {
        "as_of": _ym_to_asof(latest_ym_overall),
        "latest_ym": latest_ym_overall,
        "list_no": list_no,
        "codes": [{"code": c, "name": labels.get(c) or f"코드 {c}"} for c in ATM_ACCOUNT_CODES],
        "banks": banks,
        "total": sum(b.get("total", 0) for b in banks),
    }


def fetch_branch_stats_from_fisis():
    codes = fisis_discover_codes()
    branch = fisis_build_branch_stats(codes)
    if not branch:
        return None
    regional_data = fisis_build_regional_stats(codes)
    atm_data = fisis_build_atm_stats(codes)
    return {
        "as_of": branch["as_of"],
        "latest_ym": branch.get("latest_ym", ""),
        "source": "금융통계정보시스템(FISIS) · 국내은행 영업점포현황",
        "source_url": f"{FISIS_BASE}/statisticsInfoSearch.json",
        "banks": branch["banks"],
        "regional": (regional_data or {}).get("regional", []),
        "atm_devices": atm_data or {},
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
    prev = load_previous_stats()
    fisis = fetch_branch_stats_from_fisis()
    if fisis:
        # FISIS 지역 통계가 일시적으로 비는 날은 기존 누적 지역 데이터 유지
        if not fisis.get("regional") and prev.get("regional"):
            fisis["regional"] = prev.get("regional", [])
            fisis["latest_ym"] = fisis.get("latest_ym") or prev.get("latest_ym", "")
            fisis["source"] = f"{fisis.get('source', '')} (regional: previous cache reused)"
        if not fisis.get("atm_devices") and prev.get("atm_devices"):
            fisis["atm_devices"] = prev.get("atm_devices", {})
        return fisis

    scraped = fetch_branch_stats_from_kfb()
    if scraped:
        if prev.get("regional"):
            scraped["regional"] = prev.get("regional", [])
            scraped["latest_ym"] = prev.get("latest_ym", "")
        if prev.get("atm_devices"):
            scraped["atm_devices"] = prev.get("atm_devices", {})
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
        fallback.setdefault("atm_devices", {})
        if (not fallback.get("regional")) and prev.get("regional"):
            fallback["regional"] = prev.get("regional", [])
            fallback["latest_ym"] = prev.get("latest_ym", "")
        if (not fallback.get("atm_devices")) and prev.get("atm_devices"):
            fallback["atm_devices"] = prev.get("atm_devices", {})
        return fallback

    return {
        "as_of": datetime.now(KST).strftime("%Y-%m-%d"),
        "source": "데이터 없음 (FISIS·스크래핑 모두 실패)",
        "banks": [
            {"name": b["name"], "branches": 0, "sub_offices": 0, "count": 0} for b in TARGET_BANKS
        ],
        "regional": [],
        "atm_devices": {},
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

    # 전년말 대비 = 최신 YM의 직전 연말 값 비교
    def _prev_year_end_count(history: list, latest_ym: str) -> int:
        if not history:
            return 0
        latest_y = int(latest_ym[:4]) if latest_ym and len(latest_ym) >= 4 else 0
        latest_is_ye = bool(latest_ym and latest_ym.endswith("12"))
        target_y = (latest_y - 1) if latest_is_ye else (latest_y - 1)
        target_ym = f"{target_y}12"
        for h in history:
            if h.get("ym") == target_ym:
                return int(h.get("count", 0))
        # 없으면 latest 직전 시점 사용
        ye_points = [h for h in history if h.get("ym", "").endswith("12") and h.get("ym") < (latest_ym or "")]
        if ye_points:
            return int(ye_points[-1].get("count", 0))
        return 0

    latest_ym_str = stats.get("latest_ym", "") or ""
    banks_out = []
    for b in stats.get("banks", []):
        history = b.get("history", [])
        prev_ye = _prev_year_end_count(history, latest_ym_str)
        delta_yoy = (b["count"] - prev_ye) if prev_ye else 0
        banks_out.append({
            "name": b["name"],
            "count": b["count"],
            "branches": int(b.get("branches", b["count"])),
            "sub_offices": int(b.get("sub_offices", 0)),
            "delta_yoy": delta_yoy,
            "delta_qoq": delta_yoy,  # 하위 호환 (legacy UI key)
            "prev_year_end_count": prev_ye,
            "history": history,
        })

    total = sum(x["count"] for x in banks_out)
    total_prev_ye = sum(x["prev_year_end_count"] for x in banks_out)
    total_delta_yoy = total - total_prev_ye if total_prev_ye else 0

    stats_output = {
        "updated_at": datetime.now(KST).isoformat(),
        "as_of": stats.get("as_of"),
        "latest_ym": latest_ym_str,
        "source": stats.get("source"),
        "source_url": stats.get("source_url", ""),
        "is_fallback": bool(stats.get("is_fallback", False)),
        "banks": banks_out,
        "total": total,
        "total_delta_yoy": total_delta_yoy,
        "total_delta_qoq": total_delta_yoy,  # 하위 호환
        "regional": stats.get("regional", []),
        "atm_devices": stats.get("atm_devices", {}),
    }
    (DATA_DIR / "branch_stats.json").write_text(
        json.dumps(stats_output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA_DIR / "fisis_regional.json").write_text(
        json.dumps(
            {
                "updated_at": stats_output["updated_at"],
                "as_of": stats_output["as_of"],
                "latest_ym": stats_output.get("latest_ym", ""),
                "regional": stats_output.get("regional", []),
                "source": stats_output.get("source", ""),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[stats] saved: as_of={stats.get('as_of')} fallback={stats_output['is_fallback']} regional_n={len(stats_output['regional'])}")


if __name__ == "__main__":
    main()

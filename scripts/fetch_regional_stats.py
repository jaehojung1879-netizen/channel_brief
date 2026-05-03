"""
영업점 소재 시·군·구별 입지 분석 기초자료 수집.

- 통계청 KOSIS Open API: 인구 (시·군·구), 사업체수 (시·군·구), 평균 가구소득 (시·도)
- 한국부동산원 R-ONE Open API: 시·군·구 매매가격지수
- 서울 열린데이터광장: IoT 유동인구 센서 (자치구별, 서울만)
- 결과: data/regional_stats.json
- 비공개 키 미설정 시 graceful skip.

환경변수 (없으면 graceful skip):
  KOSIS    — 통계청 KOSIS Open API 인증키
  R_ONE    — 한국부동산원 R-ONE Open API 인증키
  SEOUL    — 서울 열린데이터광장 사용자키 (서울 자치구 유동인구 한정)
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

BRANCHES_PATH = DATA_DIR / "kakao_branches.json"
OUT_PATH = DATA_DIR / "regional_stats.json"

KOSIS_BASE = "https://kosis.kr/openapi/Param/statisticsParameterData.do"

# ---------- KOSIS 표 정의 ----------
# 행정안전부 주민등록 인구·세대수 (월별, 시군구).
KOSIS_POP_TBL = {
    "orgId": "101",
    "tblId": "DT_1B040A3",
    "itmId": "T20",
    "objL1": "ALL",
    "format": "json",
    "jsonVD": "Y",
    "prdSe": "M",
    "newEstPrdCnt": "1",
}
# 전국사업체조사 시·군·구별 사업체수 (연간). 주의: KOSIS 카탈로그에서 표 ID
# / itmId 가 변경될 수 있으니, 실패 로그가 보이면 본 정의를 갱신.
KOSIS_BIZ_TBL = {
    "orgId": "101",
    "tblId": "DT_1K52001",
    "itmId": "T01",          # 사업체수
    "objL1": "ALL",
    "format": "json",
    "jsonVD": "Y",
    "prdSe": "Y",
    "newEstPrdCnt": "1",
}
# 가계금융복지조사 시·도별 가구 평균 경상소득 (연간). 시·군·구 단위 직접
# 데이터는 KOSIS 에 사실상 없어 시·도 평균을 하위 시·군·구에 동일 적용 (proxy).
KOSIS_INCOME_TBL = {
    "orgId": "101",
    "tblId": "DT_1L9H001",
    "itmId": "T01",          # 평균 경상소득
    "objL1": "ALL",
    "format": "json",
    "jsonVD": "Y",
    "prdSe": "Y",
    "newEstPrdCnt": "1",
}

R_ONE_BASE = "https://www.reb.or.kr/r-one/openapi/SttsApiTblData.do"
R_ONE_TBL = {
    "STATBL_ID": "A_2024_00045",
    "DTACYCLE_CD": "MM",
    "Type": "json",
}

# ---------- Seoul 열린데이터광장 ----------
SEOUL_API_BASE = "http://openapi.seoul.go.kr:8088"
SEOUL_FLOATING_SERVICE = "IotVdata018"  # 시민생활 데이터 IoT 유동인구 센서
SEOUL_FLOATING_PAGE = 1000               # 한 호출 최대 행수
SEOUL_FLOATING_MAX_ROWS = 30000          # 가장 최근 N 행만 사용 (≈ 최근 6 시간 분량)

# 자치구 영문 표기 → 한글 (Seoul OpenAPI 응답은 영문 표기).
SEOUL_DISTRICT_KO = {
    "Jongno-gu": "종로구", "Jung-gu": "중구", "Yongsan-gu": "용산구",
    "Seongdong-gu": "성동구", "Gwangjin-gu": "광진구", "Dongdaemun-gu": "동대문구",
    "Jungnang-gu": "중랑구", "Seongbuk-gu": "성북구", "Gangbuk-gu": "강북구",
    "Dobong-gu": "도봉구", "Nowon-gu": "노원구", "Eunpyeong-gu": "은평구",
    "Seodaemun-gu": "서대문구", "Mapo-gu": "마포구", "Yangcheon-gu": "양천구",
    "Gangseo-gu": "강서구", "Guro-gu": "구로구", "Geumcheon-gu": "금천구",
    "Yeongdeungpo-gu": "영등포구", "Dongjak-gu": "동작구", "Gwanak-gu": "관악구",
    "Seocho-gu": "서초구", "Gangnam-gu": "강남구", "Songpa-gu": "송파구",
    "Gangdong-gu": "강동구",
}

# 광역시·도 prefix → 표준 라벨 (kakao_branches address 첫 토큰 매핑용)
SIDO_NORMALIZE = [
    ("서울특별시", "서울"), ("서울", "서울"),
    ("부산광역시", "부산"), ("부산", "부산"),
    ("대구광역시", "대구"), ("대구", "대구"),
    ("인천광역시", "인천"), ("인천", "인천"),
    ("광주광역시", "광주"), ("광주", "광주"),
    ("대전광역시", "대전"), ("대전", "대전"),
    ("울산광역시", "울산"), ("울산", "울산"),
    ("세종특별자치시", "세종"), ("세종", "세종"),
    ("경기도", "경기"), ("경기", "경기"),
    ("강원특별자치도", "강원"), ("강원도", "강원"), ("강원", "강원"),
    ("충청북도", "충북"), ("충북", "충북"),
    ("충청남도", "충남"), ("충남", "충남"),
    ("전북특별자치도", "전북"), ("전라북도", "전북"), ("전북", "전북"),
    ("전라남도", "전남"), ("전남", "전남"),
    ("경상북도", "경북"), ("경북", "경북"),
    ("경상남도", "경남"), ("경남", "경남"),
    ("제주특별자치도", "제주"), ("제주도", "제주"), ("제주", "제주"),
]


def _split_addr(addr: str) -> tuple[str, str]:
    """address_name → (시도, 시군구) 표준 라벨. 실패 시 ('', '')."""
    if not addr:
        return "", ""
    s = addr.strip()
    sido = ""
    rest = s
    for prefix, label in SIDO_NORMALIZE:
        if s.startswith(prefix):
            sido = label
            rest = s[len(prefix):].strip()
            break
    if not sido:
        return "", ""
    # 다음 토큰이 시·군·구. 단 '경기 수원시 영통구' 처럼 2단계 인 경우는 시 이름까지만.
    parts = rest.split()
    if not parts:
        return sido, ""
    sigungu = parts[0]
    # '수원시 영통구' 같은 경우 시 단위 사용
    if len(parts) >= 2 and parts[1].endswith("구") and sigungu.endswith("시"):
        sigungu = f"{sigungu} {parts[1]}"
    return sido, sigungu


def _region_key(sido: str, sigungu: str) -> str:
    if not sido:
        return ""
    if not sigungu:
        return sido
    return f"{sido} {sigungu}"


def _collect_regions() -> tuple[list[dict], dict[str, int]]:
    """kakao_branches.json 에서 unique (sido, sigungu) 목록과 영업점 수 카운트."""
    if not BRANCHES_PATH.exists():
        print(f"[skip] {BRANCHES_PATH} 가 없어 지역 추출 불가.")
        return [], {}
    try:
        payload = json.loads(BRANCHES_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[warn] kakao_branches.json 파싱 실패: {e!r}")
        return [], {}
    counts: Counter[str] = Counter()
    regions: dict[str, dict] = {}
    for b in payload.get("branches") or []:
        sido, sigungu = _split_addr(b.get("address") or b.get("road_address") or "")
        if not sido:
            continue
        key = _region_key(sido, sigungu)
        counts[key] += 1
        if key not in regions:
            regions[key] = {"key": key, "sido": sido, "sigungu": sigungu}
    region_list = sorted(regions.values(), key=lambda x: x["key"])
    return region_list, dict(counts)


def _fetch_kosis_table(api_key: str, tbl_config: dict | None = None, label: str = "kosis") -> tuple[list | None, str | None]:
    """KOSIS 표 전체를 1 회 호출로 받아온다 (인구·사업체·소득 등 공용).
    반환값: (rows, error_msg). 정상 시 error_msg=None.
    """
    params = dict(tbl_config if tbl_config is not None else KOSIS_POP_TBL)
    params["apiKey"] = api_key
    try:
        r = requests.get(KOSIS_BASE, params=params, timeout=60)
    except requests.RequestException as e:
        return None, f"{label}_request_failed: {e!r}"
    if r.status_code != 200:
        return None, f"{label}_http_{r.status_code}: {r.text[:200]}"
    try:
        body = r.json()
    except ValueError as e:
        return None, f"{label}_non_json: {e!r} body={r.text[:200]}"
    if isinstance(body, dict) and body.get("err") is not None:
        return None, f"{label}_api_error: {body!r}"[:300]
    if not isinstance(body, list):
        return None, f"{label}_unexpected_payload type={type(body).__name__} sample={str(body)[:200]}"
    return body, None


def _build_kosis_index(rows: list) -> dict[str, dict]:
    """C1_NM(공백제거) 기준으로 가장 최신 PRD_DE 의 row 만 모은 인덱스."""
    by_nm: dict[str, dict] = {}
    for row in rows:
        nm = (row.get("C1_NM") or "").replace(" ", "")
        if not nm:
            continue
        cur = by_nm.get(nm)
        if cur is None or (row.get("PRD_DE") or "") > (cur.get("PRD_DE") or ""):
            by_nm[nm] = row
    return by_nm


def _kosis_row_to_value(latest: dict, default_unit: str = "명") -> dict:
    raw = latest.get("DT")
    val: float | int | None = None
    if raw not in (None, ""):
        try:
            f = float(raw)
            val = int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            val = None
    return {
        "value": val,
        "period": latest.get("PRD_DE"),
        "label": latest.get("C1_NM"),
        "unit": latest.get("UNIT_NM") or default_unit,
    }


# ---------- Seoul OpenAPI 유동인구 ----------

def _fetch_seoul_floating(api_key: str, max_rows: int = SEOUL_FLOATING_MAX_ROWS) -> tuple[dict[str, dict], str | None]:
    """Seoul IoT 유동인구 센서 (IotVdata018) 의 가장 최근 max_rows 행을 받아
    자치구별 (한글 라벨) 평균 visitor count 산출.

    Seoul OpenAPI URL 패턴 (필터 슬롯 2개 의무):
      http://openapi.seoul.go.kr:8088/{KEY}/{TYPE}/{SERVICE}/{START}/{END}/%20/%20/
    pagination 은 1-indexed inclusive, START < END 필수, 한 호출 최대 1000 행.

    반환: (자치구_한글 → {value, samples, total, unit, source}, error_msg or None).
    """

    def _is_xml_error(text: str) -> str | None:
        """응답이 XML 에러 (<RESULT><CODE>ERROR-...) 면 메시지 추출, 아니면 None."""
        s = (text or "").lstrip()
        if not s.startswith("<"):
            return None
        m_code = re.search(r"<CODE>([^<]+)</CODE>", s)
        m_msg = re.search(r"<MESSAGE>(?:<!\[CDATA\[)?([^<\]]+)", s)
        return f"{(m_code.group(1) if m_code else 'XML_ERR')}: {(m_msg.group(1) if m_msg else '')[:200]}"

    # 1) 초기 호출로 list_total_count 얻기. Seoul OpenAPI 는 START==END 를 거부하므로
    #    1/5 로 호출 (사용자 예시 패턴과 동일). 필터 슬롯 2 개 (%20/%20) 도 추가.
    url0 = f"{SEOUL_API_BASE}/{api_key}/json/{SEOUL_FLOATING_SERVICE}/1/5/%20/%20/"
    try:
        r = requests.get(url0, timeout=30)
    except requests.RequestException as e:
        return {}, f"seoul_init_request: {e!r}"
    if r.status_code != 200:
        return {}, f"seoul_init_http_{r.status_code}: {r.text[:200]}"

    xml_err = _is_xml_error(r.text)
    if xml_err is not None:
        return {}, f"seoul_init_xml_error: {xml_err}"
    try:
        body = r.json()
    except ValueError as e:
        return {}, f"seoul_init_non_json: {e!r} body={r.text[:200]}"

    container = body.get(SEOUL_FLOATING_SERVICE) or {}
    result = container.get("RESULT") or {}
    if isinstance(result, dict):
        code = str(result.get("CODE") or "")
        if code and code not in ("INFO-000", "00", "0"):
            return {}, f"seoul_api_error: code={code} msg={result.get('MESSAGE')!s:.200}"

    try:
        total = int(container.get("list_total_count") or 0)
    except (TypeError, ValueError):
        total = 0
    if total <= 0:
        return {}, f"seoul_empty_total: keys={list(container.keys())}"

    end = total
    start = max(1, end - max_rows + 1)
    print(
        f"[fetch] Seoul 유동인구: 전체 {total} 행 중 최근 {end - start + 1} 행 호출 (start={start} end={end})",
        flush=True,
    )

    counts: dict[str, list[int]] = {}
    pages_ok = 0
    pages_fail = 0
    last_err: str | None = None
    cur = start
    while cur <= end:
        cur_end = min(cur + SEOUL_FLOATING_PAGE - 1, end)
        # START==END 회피 — 한 행만 남으면 한 칸 앞당겨 호출 (중복 방지를 위해 dedupe 는 SERIAL_NO+SENSING_TIME 으로).
        if cur == cur_end and cur > 1:
            cur -= 1
        url = f"{SEOUL_API_BASE}/{api_key}/json/{SEOUL_FLOATING_SERVICE}/{cur}/{cur_end}/%20/%20/"
        try:
            r = requests.get(url, timeout=30)
        except requests.RequestException as e:
            last_err = f"page_{cur}_{cur_end}_req: {e!r}"
            pages_fail += 1
            cur = cur_end + 1
            continue
        xml_err = _is_xml_error(r.text)
        if xml_err is not None:
            last_err = f"page_{cur}_{cur_end}_xml: {xml_err}"
            pages_fail += 1
            cur = cur_end + 1
            continue
        try:
            body = r.json()
        except ValueError as e:
            last_err = f"page_{cur}_{cur_end}_json: {e!r}"
            pages_fail += 1
            cur = cur_end + 1
            continue

        rows = (body.get(SEOUL_FLOATING_SERVICE) or {}).get("row") or []
        if not isinstance(rows, list):
            rows = []
        for row in rows:
            district_en = (row.get("AUTONOMOUS_DISTRICT") or "").strip()
            district_ko = SEOUL_DISTRICT_KO.get(district_en)
            if not district_ko:
                continue
            try:
                v = int(row.get("VISITOR_COUNT") or 0)
            except (ValueError, TypeError):
                continue
            if v < 0:
                continue
            counts.setdefault(district_ko, []).append(v)
        pages_ok += 1
        cur = cur_end + 1

    out: dict[str, dict] = {}
    now_kst = datetime.now(KST).strftime("%Y-%m-%d")
    for district_ko, vs in counts.items():
        if not vs:
            continue
        out[district_ko] = {
            "value": round(sum(vs) / len(vs), 2),
            "total": sum(vs),
            "samples": len(vs),
            "period": now_kst,
            "unit": "명/측정",
            "source": "Seoul OpenAPI · IotVdata018",
        }
    print(
        f"[fetch] Seoul 유동인구 처리 완료: {pages_ok} 페이지 성공 / {pages_fail} 실패, "
        f"{len(out)} 자치구 집계",
        flush=True,
    )
    return out, last_err


def _fetch_rone_table(api_key: str) -> tuple[list | None, str | None]:
    """R-ONE 시·군·구 매매가격지수 표 전체를 페이징으로 모두 받아온다.
    R-ONE OpenAPI 는 pIndex / pSize 를 받기 때문에, 누락하면 일부 시·군·구가 응답에서
    빠질 수 있다. 한 페이지 1000행씩 받아 모두 누적한다.
    """
    PAGE_SIZE = 1000
    rows_all: list = []
    page = 1
    last_err: str | None = None
    while True:
        params = dict(R_ONE_TBL)
        params["KEY"] = api_key
        params["pIndex"] = str(page)
        params["pSize"] = str(PAGE_SIZE)
        try:
            r = requests.get(R_ONE_BASE, params=params, timeout=60)
        except requests.RequestException as e:
            last_err = f"request_failed page={page}: {e!r}"
            break
        if r.status_code != 200:
            last_err = f"http_{r.status_code} page={page}: {r.text[:200]}"
            break
        try:
            body = r.json()
        except ValueError as e:
            last_err = f"non_json_response page={page}: {e!r} body={r.text[:200]}"
            break

        # R-ONE 인증 실패 / 한도 초과 등은 dict 의 RESULT 필드에 메시지가 들어온다.
        if isinstance(body, dict):
            result = body.get("RESULT") or body.get("result")
            if isinstance(result, dict):
                code = str(result.get("CODE") or result.get("resultCode") or "")
                msg = str(result.get("MESSAGE") or result.get("resultMsg") or "")
                if code and code not in ("INFO-000", "00", "0"):
                    last_err = f"api_error page={page}: code={code} msg={msg[:200]}"
                    break

        rows: list = []
        if isinstance(body, dict):
            for v in body.values():
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict) and isinstance(item.get("row"), list):
                            rows = item["row"]
                            break
                if rows:
                    break
        if not rows:
            # 첫 페이지에서 비어있으면 진짜 빈 응답으로 간주.
            if page == 1:
                last_err = f"empty_response page=1 body_keys={list(body.keys()) if isinstance(body, dict) else type(body).__name__}"
            break
        rows_all.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        page += 1
        if page > 50:
            print(f"[warn] R-ONE 페이지가 50을 초과 — 중단 (현재까지 {len(rows_all)} 행 수집)", flush=True)
            break
    if not rows_all:
        return None, last_err or "no_rows"
    return rows_all, None


def _build_rone_index(rows: list) -> dict[str, dict]:
    """R-ONE row 인덱스 (시·군·구명 공백제거 → 가장 최신 시점의 row)."""
    by_nm: dict[str, dict] = {}
    for row in rows:
        nm = (row.get("CLS_NM") or row.get("REGION_NM") or row.get("REGION_CD_NM") or "").replace(" ", "")
        if not nm:
            continue
        t = row.get("WRTTIME_IDTFR_ID") or row.get("WRTTIME_DESC") or ""
        cur = by_nm.get(nm)
        if cur is None:
            by_nm[nm] = row
            continue
        cur_t = cur.get("WRTTIME_IDTFR_ID") or cur.get("WRTTIME_DESC") or ""
        if t > cur_t:
            by_nm[nm] = row
    return by_nm


def _rone_row_to_value(latest: dict) -> dict:
    val = latest.get("DTA_VAL")
    try:
        val_f = float(val) if val is not None else None
    except (TypeError, ValueError):
        val_f = None
    return {
        "value": val_f,
        "period": latest.get("WRTTIME_IDTFR_ID") or latest.get("WRTTIME_DESC"),
        "label": latest.get("CLS_NM") or latest.get("REGION_NM"),
        "unit": "지수(2021.06=100)",
    }


# R-ONE 표가 너무 오래된 시점만 반환할 경우 (예: 2005년) 표시는 무의미하므로,
# 최근 N 년 이내 시점만 유효 데이터로 인정.
PRICE_INDEX_MAX_AGE_MONTHS = 24


def _is_period_recent_enough(period: str | None) -> bool:
    """WRTTIME_IDTFR_ID (보통 'YYYYMM') 가 최근 PRICE_INDEX_MAX_AGE_MONTHS 이내인지."""
    if not period:
        return False
    s = str(period).strip()
    # YYYYMM
    if len(s) >= 6 and s[:6].isdigit():
        try:
            y, m = int(s[:4]), int(s[4:6])
            now = datetime.now(KST)
            age = (now.year - y) * 12 + (now.month - m)
            return 0 <= age <= PRICE_INDEX_MAX_AGE_MONTHS
        except ValueError:
            return False
    return False


def _lookup_region(index: dict[str, dict], region: dict) -> dict | None:
    """로컬 인덱스에서 시·군·구 row 1 건 조회 (정확 일치 → 부분 포함 순)."""
    sido = region["sido"]
    sigungu = region["sigungu"]
    target = (sigungu or sido).replace(" ", "")
    if not target:
        return None
    if target in index:
        return index[target]
    for nm, row in index.items():
        if target in nm or nm in target:
            return row
    return None


def _load_existing() -> dict:
    """regional_stats.json 이 이미 있다면 로드. 없으면 빈 구조."""
    if not OUT_PATH.exists():
        return {}
    try:
        return json.loads(OUT_PATH.read_text(encoding="utf-8")) or {}
    except (OSError, ValueError) as e:
        print(f"[warn] 기존 regional_stats.json 파싱 실패: {e!r} — 새로 생성합니다.")
        return {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="시·군·구 지역통계 수집 (KOSIS / R-ONE).")
    parser.add_argument(
        "--source",
        choices=("kosis", "r_one", "both"),
        default="both",
        help=(
            "수집 대상. 'kosis' 는 인구통계만, 'r_one' 은 매매가격지수만, "
            "'both' 는 기존과 동일하게 둘 다 (기본값)."
        ),
    )
    args = parser.parse_args(argv)

    do_kosis = args.source in ("kosis", "both")
    do_rone = args.source in ("r_one", "both")

    kosis_key = (os.environ.get("KOSIS") or "").strip() if do_kosis else ""
    rone_key = (os.environ.get("R_ONE") or "").strip() if do_rone else ""

    regions, counts = _collect_regions()
    if not regions:
        OUT_PATH.write_text(json.dumps({
            "as_of": datetime.now(KST).isoformat(timespec="seconds"),
            "regions": {},
            "diagnostics": {"note": "kakao_branches.json 에서 지역 추출 실패"},
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    # 기존 결과를 읽어와서 source 별 필드만 갱신 (다른 source 데이터는 보존).
    existing = _load_existing()
    existing_regions: dict[str, dict] = existing.get("regions") or {}
    existing_diag: dict = existing.get("diagnostics") or {}

    diagnostics = {
        "source": args.source,
        "kosis_key": "set" if kosis_key else ("skip" if not do_kosis else "empty"),
        "r_one_key": "set" if rone_key else ("skip" if not do_rone else "empty"),
        "region_count": len(regions),
    }

    if do_kosis and not kosis_key and do_rone and not rone_key:
        # 둘 다 비어있는 'both' 단독 실행
        print("[skip] KOSIS / R_ONE 키 모두 미설정 — 지역 통계 수집을 건너뜁니다.")
        OUT_PATH.write_text(json.dumps({
            "as_of": datetime.now(KST).isoformat(timespec="seconds"),
            "regions": existing_regions,
            "diagnostics": {**diagnostics, "note": "KOSIS / R_ONE secret 등록 후 다음 워크플로 실행 시 반영됩니다."},
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0
    if args.source == "kosis" and not kosis_key:
        print("[skip] KOSIS 키 미설정 — KOSIS 단독 수집 건너뜀.")
        return 0
    if args.source == "r_one" and not rone_key:
        print("[skip] R_ONE 키 미설정 — R-ONE 단독 수집 건너뜀.")
        return 0

    # KOSIS 워크플로 (kosis / both) 에 한해 서울 유동인구도 동시에 수집.
    seoul_key = (os.environ.get("SEOUL") or "").strip() if do_kosis else ""
    diagnostics["seoul_key"] = "set" if seoul_key else ("skip" if not do_kosis else "empty")

    # 인덱스 준비.
    kosis_pop_index: dict[str, dict] = {}
    kosis_biz_index: dict[str, dict] = {}
    kosis_income_sido_index: dict[str, dict] = {}
    rone_index: dict[str, dict] = {}
    seoul_floating: dict[str, dict] = {}

    if kosis_key:
        print("[fetch] KOSIS 시·군·구별 인구 표 호출…", flush=True)
        rows, err = _fetch_kosis_table(kosis_key, KOSIS_POP_TBL, label="kosis_pop")
        if err:
            print(f"[error] KOSIS 인구 fetch 실패: {err}", flush=True)
            diagnostics["kosis_pop_error"] = err
        else:
            kosis_pop_index = _build_kosis_index(rows or [])
            print(f"[fetch] KOSIS 인구 rows={len(rows or [])}, unique 지역={len(kosis_pop_index)}", flush=True)

        print("[fetch] KOSIS 시·군·구별 사업체수 표 호출…", flush=True)
        rows, err = _fetch_kosis_table(kosis_key, KOSIS_BIZ_TBL, label="kosis_biz")
        if err:
            print(f"[error] KOSIS 사업체수 fetch 실패: {err}", flush=True)
            diagnostics["kosis_biz_error"] = err
        else:
            kosis_biz_index = _build_kosis_index(rows or [])
            print(f"[fetch] KOSIS 사업체수 rows={len(rows or [])}, unique 지역={len(kosis_biz_index)}", flush=True)

        print("[fetch] KOSIS 시·도별 평균 가구 경상소득 표 호출…", flush=True)
        rows, err = _fetch_kosis_table(kosis_key, KOSIS_INCOME_TBL, label="kosis_income")
        if err:
            print(f"[error] KOSIS 소득 fetch 실패: {err}", flush=True)
            diagnostics["kosis_income_error"] = err
        else:
            kosis_income_sido_index = _build_kosis_index(rows or [])
            print(f"[fetch] KOSIS 소득 rows={len(rows or [])}, unique 시·도={len(kosis_income_sido_index)}", flush=True)

    if rone_key:
        print("[fetch] R-ONE 시·군·구 매매가격지수 표 페이징 호출…", flush=True)
        rows, err = _fetch_rone_table(rone_key)
        if err and not rows:
            print(f"[error] R-ONE fetch 실패: {err}", flush=True)
            diagnostics["r_one_error"] = err
        else:
            if err:
                print(f"[warn] R-ONE 부분 실패: {err}", flush=True)
                diagnostics["r_one_warn"] = err
            rone_index = _build_rone_index(rows or [])
            print(f"[fetch] R-ONE rows={len(rows or [])}, unique 지역={len(rone_index)}", flush=True)

    if seoul_key:
        try:
            seoul_floating, err = _fetch_seoul_floating(seoul_key)
            if err:
                print(f"[warn] Seoul 유동인구 부분 실패: {err}", flush=True)
                diagnostics["seoul_warn"] = err
            diagnostics["seoul_districts"] = len(seoul_floating)
        except requests.RequestException as e:  # pragma: no cover — 안전망
            print(f"[error] Seoul 유동인구 호출 예외: {e!r}", flush=True)
            diagnostics["seoul_error"] = repr(e)[:200]

    out: dict[str, dict] = {}
    pop_ok = pop_fail = 0
    biz_ok = biz_fail = 0
    income_ok = income_fail = 0
    rone_ok = rone_fail = 0
    flow_ok = 0

    for region in regions:
        prev = existing_regions.get(region["key"]) or {}
        entry: dict = {
            "sido": region["sido"],
            "sigungu": region["sigungu"],
            "branch_count": counts.get(region["key"], 0),
        }
        # 미실행 source 의 데이터는 기존 값 그대로 보존.
        if not do_kosis:
            for k in ("population", "businesses", "income"):
                if k in prev:
                    entry[k] = prev[k]
            if "floating_population" in prev:
                entry["floating_population"] = prev["floating_population"]
        if not do_rone and "price_index" in prev:
            entry["price_index"] = prev["price_index"]

        # KOSIS 인구
        if kosis_key:
            row = _lookup_region(kosis_pop_index, region)
            if row is not None:
                entry["population"] = _kosis_row_to_value(row, default_unit="명")
                pop_ok += 1
            else:
                if "population" in prev:
                    entry["population"] = prev["population"]
                pop_fail += 1

        # KOSIS 사업체수
        if kosis_key and kosis_biz_index:
            row = _lookup_region(kosis_biz_index, region)
            if row is not None:
                entry["businesses"] = _kosis_row_to_value(row, default_unit="개")
                biz_ok += 1
            else:
                if "businesses" in prev:
                    entry["businesses"] = prev["businesses"]
                biz_fail += 1

        # KOSIS 소득 (시·도 평균을 하위 시·군·구에 부여 — proxy)
        if kosis_key and kosis_income_sido_index:
            sido = region["sido"]
            sido_target = sido.replace(" ", "")
            sido_row = kosis_income_sido_index.get(sido_target)
            if sido_row is None:
                # partial match
                for nm, r_ in kosis_income_sido_index.items():
                    if sido_target and (sido_target in nm or nm in sido_target):
                        sido_row = r_
                        break
            if sido_row is not None:
                v = _kosis_row_to_value(sido_row, default_unit="만원")
                v["scope"] = "sido"
                v["note"] = f"{sido} 시·도 평균 (시·군·구 직접 데이터 부재)"
                entry["income"] = v
                income_ok += 1
            else:
                if "income" in prev:
                    entry["income"] = prev["income"]
                income_fail += 1

        # 서울 자치구 유동인구
        if kosis_key and seoul_floating and region["sido"] == "서울":
            sigungu_target = region["sigungu"].replace(" ", "")
            fp = seoul_floating.get(sigungu_target)
            if fp is not None:
                entry["floating_population"] = fp
                flow_ok += 1

        # R-ONE 매매가격지수 (옛 스냅샷이면 폐기)
        if rone_key:
            row = _lookup_region(rone_index, region)
            if row is not None:
                pi = _rone_row_to_value(row)
                if _is_period_recent_enough(pi.get("period")):
                    entry["price_index"] = pi
                    rone_ok += 1
                else:
                    rone_fail += 1
            else:
                if "price_index" in prev:
                    entry["price_index"] = prev["price_index"]
                rone_fail += 1

        out[region["key"]] = entry

    print(
        f"[summary] {len(regions)} 지역 처리: pop ok={pop_ok}/fail={pop_fail}, "
        f"biz ok={biz_ok}/fail={biz_fail}, income ok={income_ok}/fail={income_fail}, "
        f"flow(서울) ok={flow_ok}, R-ONE ok={rone_ok}/fail={rone_fail}",
        flush=True,
    )

    if do_kosis:
        diagnostics["population_ok"] = pop_ok
        diagnostics["population_fail"] = pop_fail
        diagnostics["businesses_ok"] = biz_ok
        diagnostics["businesses_fail"] = biz_fail
        diagnostics["income_ok"] = income_ok
        diagnostics["income_fail"] = income_fail
        if seoul_key:
            diagnostics["floating_ok"] = flow_ok
    else:
        for k in ("population_ok", "population_fail",
                  "businesses_ok", "businesses_fail",
                  "income_ok", "income_fail", "floating_ok"):
            if k in existing_diag:
                diagnostics[k] = existing_diag[k]
    if do_rone:
        diagnostics["price_index_ok"] = rone_ok
        diagnostics["price_index_fail"] = rone_fail
    else:
        for k in ("price_index_ok", "price_index_fail"):
            if k in existing_diag:
                diagnostics[k] = existing_diag[k]

    _attach_location_scores(out)

    OUT_PATH.write_text(json.dumps({
        "as_of": datetime.now(KST).isoformat(timespec="seconds"),
        "regions": out,
        "diagnostics": diagnostics,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] regional_stats.json 갱신 ({args.source}): {len(out)} 지역", flush=True)
    return 0


def _attach_location_scores(regions_out: dict[str, dict]) -> None:
    """각 시·군·구에 0~100 입지 점수 (location_score) 부여.

    가중치 (실무 경험 기반):
      - 사업체수 (KOSIS, 시·군·구):   45 %
      - 가구 평균소득 (KOSIS, 시·도):  35 %
      - 시장 규모: 20 %
          서울 자치구는 'floating_population' (Seoul OpenAPI IoT 유동인구) 사용,
          그 외 시·군·구는 'population' 사용.

    각 컴포넌트는 모든 시·군·구 대비 log 변환 백분위 (0~100). 누락 컴포넌트는
    제외하고 남은 가중치를 정규화해 점수 산출. 컴포넌트가 하나도 없으면
    point None.
    """

    def _log_pct(idx_values: list[tuple[int, float]]) -> dict[int, float]:
        """[(원본인덱스, 값)] → 인덱스 → 백분위 (0~100)."""
        if not idx_values:
            return {}
        logs = [(idx, math.log(max(v, 1.0))) for idx, v in idx_values]
        logs.sort(key=lambda p: p[1])
        n = len(logs)
        return {idx: (order / max(n - 1, 1)) * 100.0 for order, (idx, _) in enumerate(logs)}

    keys = list(regions_out.keys())

    def _collect(field: str, sub: str = "value") -> list[tuple[int, float]]:
        out_l: list[tuple[int, float]] = []
        for i, k in enumerate(keys):
            e = regions_out[k]
            obj = e.get(field) or {}
            v = obj.get(sub) if isinstance(obj, dict) else None
            if isinstance(v, (int, float)) and v > 0:
                out_l.append((i, float(v)))
        return out_l

    biz_pct = _log_pct(_collect("businesses"))
    income_pct = _log_pct(_collect("income"))
    pop_pct = _log_pct(_collect("population"))
    flow_pct = _log_pct(_collect("floating_population"))

    # 점포수 (4 대銀) 는 score 에는 포함하지 않지만, 컴포넌트 분해용 참고치로 노출.
    branch_idx_values = [
        (i, float(regions_out[k].get("branch_count")))
        for i, k in enumerate(keys)
        if isinstance(regions_out[k].get("branch_count"), (int, float))
        and regions_out[k]["branch_count"] > 0
    ]
    branch_pct = _log_pct(branch_idx_values)

    W_BIZ = 0.45
    W_INCOME = 0.35
    W_MARKET = 0.20  # 인구 또는 유동인구

    for i, k in enumerate(keys):
        e = regions_out[k]
        is_seoul = e.get("sido") == "서울"
        # 서울이면 유동인구 우선, 없으면 인구로 fallback. 비서울은 인구.
        market_pct: float | None = None
        market_source: str | None = None
        if is_seoul and i in flow_pct:
            market_pct = flow_pct[i]
            market_source = "floating_population"
        elif i in pop_pct:
            market_pct = pop_pct[i]
            market_source = "population"

        components: list[tuple[float, float]] = []
        if i in biz_pct:
            components.append((W_BIZ, biz_pct[i]))
        if i in income_pct:
            components.append((W_INCOME, income_pct[i]))
        if market_pct is not None:
            components.append((W_MARKET, market_pct))

        if not components:
            # Fallback: 주요 컴포넌트 모두 없을 때만 점포밀도를 임시 점수로 사용
            # (워크플로 첫 실행 직후 데이터가 차오르기 전 UI 가 비지 않도록).
            if i in branch_pct:
                e["location_score"] = {
                    "value": round(branch_pct[i], 1),
                    "components": {
                        "businesses_pct": None,
                        "income_pct": None,
                        "market_pct": None,
                        "market_source": None,
                        "branch_density_pct": round(branch_pct[i], 1),
                    },
                    "weights": {"branch_density_fallback": 1.0},
                    "note": "사업체수·소득·시장 데이터 미수집 — 임시로 4대銀 점포밀도 백분위로 대체.",
                }
            else:
                e["location_score"] = None
            continue
        total_w = sum(w for w, _ in components)
        score = sum(w * v for w, v in components) / total_w
        e["location_score"] = {
            "value": round(score, 1),
            "components": {
                "businesses_pct": round(biz_pct[i], 1) if i in biz_pct else None,
                "income_pct": round(income_pct[i], 1) if i in income_pct else None,
                "market_pct": round(market_pct, 1) if market_pct is not None else None,
                "market_source": market_source,
                "branch_density_pct": round(branch_pct[i], 1) if i in branch_pct else None,
            },
            "weights": {"businesses": W_BIZ, "income": W_INCOME, "market": W_MARKET},
            "note": (
                "사업체수 45% · 가구소득 35% · "
                "시장(서울:유동인구 / 비서울:인구) 20% — 모든 시·군·구 백분위 가중평균. "
                "누락 컴포넌트는 가중치 정규화로 제외."
            ),
        }


if __name__ == "__main__":
    sys.exit(main())

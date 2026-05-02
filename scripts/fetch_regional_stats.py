"""
영업점 소재 시·군·구별 입지 분석 기초자료 수집.

- 통계청 KOSIS Open API 에서 시·군·구별 주민등록 인구·세대 수,
- 한국부동산원 R-ONE 부동산통계정보 Open API 에서 시·군·구 단위 매매가격지수
  (또는 가능 시 평균지가 / 임대료 지수) 를 받아 옵니다.
- 결과: data/regional_stats.json
- 비공개 키가 미설정이면 graceful skip 합니다 (frontend 는 '미수집' 표기).

환경변수 (없으면 graceful skip):
  KOSIS    — 통계청 KOSIS Open API 인증키
  R_ONE    — 한국부동산원 R-ONE Open API 인증키
"""
from __future__ import annotations

import argparse
import json
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
# 행정안전부 주민등록 인구·세대수 (월별, 시군구) — 가장 일반적으로 쓰이는 표.
KOSIS_POP_TBL = {
    "orgId": "101",          # 통계청
    "tblId": "DT_1B040A3",   # 시군구별 주민등록인구 (월별)
    "itmId": "T20",          # 총인구
    "objL1": "ALL",
    "format": "json",
    "jsonVD": "Y",
    "prdSe": "M",
    "newEstPrdCnt": "1",
}

R_ONE_BASE = "https://www.reb.or.kr/r-one/openapi/SttsApiTblData.do"
# 시군구 매매가격지수 (월) — STATBL_ID 는 R-ONE 사이트에서 확인 가능.
R_ONE_TBL = {
    "STATBL_ID": "A_2024_00045",   # 시군구 종합 매매가격지수 (월간)
    "DTACYCLE_CD": "MM",
    "Type": "json",
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


def _fetch_kosis_table(api_key: str) -> tuple[list | None, str | None]:
    """KOSIS 시·군·구별 주민등록 인구 표 전체를 1 회 호출로 받아온다.
    반환값: (rows, error_msg). 정상 시 error_msg=None.
    """
    params = dict(KOSIS_POP_TBL)
    params["apiKey"] = api_key
    try:
        r = requests.get(KOSIS_BASE, params=params, timeout=60)
    except requests.RequestException as e:
        return None, f"request_failed: {e!r}"
    if r.status_code != 200:
        return None, f"http_{r.status_code}: {r.text[:200]}"
    try:
        body = r.json()
    except ValueError as e:
        return None, f"non_json_response: {e!r} body={r.text[:200]}"
    if isinstance(body, dict) and body.get("err") is not None:
        # KOSIS 가 인증 실패 등에서 dict 로 에러 반환하는 경우.
        return None, f"api_error: {body!r}"[:300]
    if not isinstance(body, list):
        return None, f"unexpected_payload type={type(body).__name__} sample={str(body)[:200]}"
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


def _kosis_row_to_value(latest: dict) -> dict:
    try:
        val = int(float(latest.get("DT") or 0))
    except (TypeError, ValueError):
        val = None
    return {
        "value": val,
        "period": latest.get("PRD_DE"),
        "label": latest.get("C1_NM"),
        "unit": latest.get("UNIT_NM") or "명",
    }


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

    # 표 전체를 1 회 (R-ONE 은 페이징) 만 받아 로컬 인덱스로 룩업.
    kosis_index: dict[str, dict] = {}
    rone_index: dict[str, dict] = {}
    if kosis_key:
        print("[fetch] KOSIS 시·군·구별 인구 표 호출…", flush=True)
        rows, err = _fetch_kosis_table(kosis_key)
        if err:
            print(f"[error] KOSIS fetch 실패: {err}", flush=True)
            diagnostics["kosis_error"] = err
        else:
            kosis_index = _build_kosis_index(rows or [])
            print(f"[fetch] KOSIS rows={len(rows or [])}, unique 지역={len(kosis_index)}", flush=True)
    if rone_key:
        print("[fetch] R-ONE 시·군·구 매매가격지수 표 페이징 호출…", flush=True)
        rows, err = _fetch_rone_table(rone_key)
        if err and not rows:
            print(f"[error] R-ONE fetch 실패: {err}", flush=True)
            diagnostics["r_one_error"] = err
        else:
            if err:
                # 일부 페이지만 실패한 경우는 경고만 남기고 진행.
                print(f"[warn] R-ONE 부분 실패: {err}", flush=True)
                diagnostics["r_one_warn"] = err
            rone_index = _build_rone_index(rows or [])
            print(f"[fetch] R-ONE rows={len(rows or [])}, unique 지역={len(rone_index)}", flush=True)

    out: dict[str, dict] = {}
    pop_ok = pop_fail = rone_ok = rone_fail = 0
    for region in regions:
        prev = existing_regions.get(region["key"]) or {}
        entry: dict = {
            "sido": region["sido"],
            "sigungu": region["sigungu"],
            "branch_count": counts.get(region["key"], 0),
        }
        # 미실행 source 의 데이터는 기존 값 그대로 보존
        if "population" in prev and not do_kosis:
            entry["population"] = prev["population"]
        if "price_index" in prev and not do_rone:
            entry["price_index"] = prev["price_index"]

        if kosis_key:
            row = _lookup_region(kosis_index, region)
            if row is not None:
                entry["population"] = _kosis_row_to_value(row)
                pop_ok += 1
            else:
                if "population" in prev:
                    entry["population"] = prev["population"]
                pop_fail += 1
        if rone_key:
            row = _lookup_region(rone_index, region)
            if row is not None:
                entry["price_index"] = _rone_row_to_value(row)
                rone_ok += 1
            else:
                if "price_index" in prev:
                    entry["price_index"] = prev["price_index"]
                rone_fail += 1
        out[region["key"]] = entry
    print(
        f"[summary] {len(regions)} 지역 처리: KOSIS ok={pop_ok}/fail={pop_fail}, "
        f"R-ONE ok={rone_ok}/fail={rone_fail}",
        flush=True,
    )

    if do_kosis:
        diagnostics["population_ok"] = pop_ok
        diagnostics["population_fail"] = pop_fail
    else:
        # 이전 진단치 유지
        if "population_ok" in existing_diag:
            diagnostics["population_ok"] = existing_diag["population_ok"]
        if "population_fail" in existing_diag:
            diagnostics["population_fail"] = existing_diag["population_fail"]
    if do_rone:
        diagnostics["price_index_ok"] = rone_ok
        diagnostics["price_index_fail"] = rone_fail
    else:
        if "price_index_ok" in existing_diag:
            diagnostics["price_index_ok"] = existing_diag["price_index_ok"]
        if "price_index_fail" in existing_diag:
            diagnostics["price_index_fail"] = existing_diag["price_index_fail"]

    OUT_PATH.write_text(json.dumps({
        "as_of": datetime.now(KST).isoformat(timespec="seconds"),
        "regions": out,
        "diagnostics": diagnostics,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] regional_stats.json 갱신 ({args.source}): {len(out)} 지역 (pop ok={pop_ok}, rone ok={rone_ok})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

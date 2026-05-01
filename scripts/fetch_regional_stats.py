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

import json
import os
import re
import sys
import time
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


def _fetch_kosis_population(api_key: str, region: dict) -> dict | None:
    """
    KOSIS '시군구별 주민등록 인구' 표에서 해당 시·군·구의 최근 월 총인구를 가져온다.
    KOSIS 는 objL1 코드를 별도로 매핑해야 하므로 본 구현은 경량 버전:
    - 표 전체를 가져와 region label 과 분류 명칭을 부분 매칭.
    """
    params = dict(KOSIS_POP_TBL)
    params["apiKey"] = api_key
    try:
        r = requests.get(KOSIS_BASE, params=params, timeout=15)
        if r.status_code != 200:
            return {"error": f"http_{r.status_code}", "raw": r.text[:120]}
        body = r.json()
    except (requests.RequestException, ValueError) as e:
        return {"error": str(e)[:120]}
    if not isinstance(body, list):
        return {"error": "unexpected_payload"}

    # KOSIS 응답: list of dicts with 'C1_NM' (지역명), 'DT' (값), 'PRD_DE' (시점)
    sido = region["sido"]
    sigungu = region["sigungu"]
    cands = []
    for row in body:
        nm = (row.get("C1_NM") or "").replace(" ", "")
        target = (sigungu or sido).replace(" ", "")
        if not target:
            continue
        if target in nm or nm in target:
            cands.append(row)
    if not cands:
        return None
    latest = max(cands, key=lambda r: r.get("PRD_DE") or "")
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


def _fetch_rone_price_index(api_key: str, region: dict) -> dict | None:
    """R-ONE 시군구 매매가격지수에서 최근 시점 값."""
    params = dict(R_ONE_TBL)
    params["KEY"] = api_key
    try:
        r = requests.get(R_ONE_BASE, params=params, timeout=20)
        if r.status_code != 200:
            return {"error": f"http_{r.status_code}", "raw": r.text[:120]}
        body = r.json()
    except (requests.RequestException, ValueError) as e:
        return {"error": str(e)[:120]}

    # R-ONE 응답 컨테이너는 통계표마다 다름. 가장 흔한 구조:
    #   {"SttsApiTblData":[{"head":[...]}, {"row":[{...}]}]}
    rows = []
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
        return None

    sido = region["sido"]
    sigungu = region["sigungu"]
    target = (sigungu or sido).replace(" ", "")
    cands = []
    for row in rows:
        nm = (row.get("CLS_NM") or row.get("REGION_NM") or row.get("REGION_CD_NM") or "").replace(" ", "")
        if target and (target in nm or nm in target):
            cands.append(row)
    if not cands:
        return None
    latest = max(cands, key=lambda r: r.get("WRTTIME_IDTFR_ID") or r.get("WRTTIME_DESC") or "")
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


def main() -> int:
    kosis_key = (os.environ.get("KOSIS") or "").strip()
    rone_key = (os.environ.get("R_ONE") or "").strip()

    regions, counts = _collect_regions()
    if not regions:
        OUT_PATH.write_text(json.dumps({
            "as_of": datetime.now(KST).isoformat(timespec="seconds"),
            "regions": {},
            "diagnostics": {"note": "kakao_branches.json 에서 지역 추출 실패"},
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    diagnostics = {
        "kosis_key": "set" if kosis_key else "empty",
        "r_one_key": "set" if rone_key else "empty",
        "region_count": len(regions),
    }

    if not kosis_key and not rone_key:
        print("[skip] KOSIS / R_ONE 키 모두 미설정 — 지역 통계 수집을 건너뜁니다.")
        OUT_PATH.write_text(json.dumps({
            "as_of": datetime.now(KST).isoformat(timespec="seconds"),
            "regions": {},
            "diagnostics": {**diagnostics, "note": "KOSIS / R_ONE secret 등록 후 다음 워크플로 실행 시 반영됩니다."},
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    out: dict[str, dict] = {}
    pop_ok = pop_fail = rone_ok = rone_fail = 0
    for idx, region in enumerate(regions, start=1):
        entry: dict = {
            "sido": region["sido"],
            "sigungu": region["sigungu"],
            "branch_count": counts.get(region["key"], 0),
        }
        if kosis_key:
            data = _fetch_kosis_population(kosis_key, region)
            if data and "error" not in data:
                entry["population"] = data
                pop_ok += 1
            else:
                pop_fail += 1
            time.sleep(0.05)
        if rone_key:
            data = _fetch_rone_price_index(rone_key, region)
            if data and "error" not in data:
                entry["price_index"] = data
                rone_ok += 1
            else:
                rone_fail += 1
            time.sleep(0.05)
        out[region["key"]] = entry
        if idx % 25 == 0 or idx == len(regions):
            print(f"  [progress] regional stats: {idx}/{len(regions)} (pop ok={pop_ok}, rone ok={rone_ok})", flush=True)

    diagnostics.update({
        "population_ok": pop_ok,
        "population_fail": pop_fail,
        "price_index_ok": rone_ok,
        "price_index_fail": rone_fail,
    })
    OUT_PATH.write_text(json.dumps({
        "as_of": datetime.now(KST).isoformat(timespec="seconds"),
        "regions": out,
        "diagnostics": diagnostics,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] regional_stats.json 갱신: {len(out)} 지역 (pop ok={pop_ok}, rone ok={rone_ok})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

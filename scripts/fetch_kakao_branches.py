"""
Kakao Local API → 4대 시중은행(신한·KB국민·하나·우리) 영업점 좌표 수집.

- 카테고리 BK9(은행) + 키워드 검색을 시·도/시·군 anchor 별로 수행 후 dedupe.
- ATM, 디지털라운지, 365자동화코너, 무인점포 등 영업점이 아닌 무인 채널은 제외.
- 결과: data/kakao_branches.json
- 같은 워크플로 안에서 KAKAO_JS_KEY 도 같이 받아 data/kakao_config.json 에
  프론트엔드용 JS 키를 기록한다 (Kakao 콘솔에서 도메인 제한 필수).

환경변수 (없으면 graceful skip):
  KAKAO_REST_API_KEY  (또는 KAKAO_REST_API / KAKAO_REST_KEY / KAKAO_API_KEY) — 서버 호출용
  KAKAO_JS_KEY        (또는 KAKAO_JAVASCRIPT_KEY / KAKAO_MAP_JS_KEY) — 프론트 SDK 용
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

KAKAO_KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
PAGE_SIZE = 15
MAX_PAGES = 3   # Kakao keyword API: 최대 45건 (3페이지)
THROTTLE_SEC = 0.06

TARGET_BANKS = [
    {
        "name": "신한",
        "official": "신한은행",
        "match_terms": ["신한은행", "신한"],
    },
    {
        "name": "KB국민",
        "official": "KB국민은행",
        "match_terms": ["KB국민은행", "국민은행", "KB국민", "KB"],
    },
    {
        "name": "하나",
        "official": "하나은행",
        "match_terms": ["하나은행", "KEB하나은행", "KEB하나", "하나"],
    },
    {
        "name": "우리",
        "official": "우리은행",
        "match_terms": ["우리은행", "우리"],
    },
]

# 영업점이 아닌 무인 채널 / ATM 식별 패턴
EXCLUDE_NAME_RE = re.compile(
    r"(?ix)"
    r"ATM|"
    r"365\s*자동|"
    r"자동화\s*코너|자동화\s*기기|"
    r"디지털\s*라운지|디지털라운지|디라운지|"
    r"디지털\s*뱅크|디지털\s*키오스크|"
    r"무인\s*점포|무인\s*창구|키오스크|"
    r"환전\s*소|환전키오스크|"
    r"외환\s*ATM|"
    r"스마트\s*ATM|"
    r"공항\s*환전"
)
EXCLUDE_CAT_RE = re.compile(r"자동화기기|환전소|키오스크")

# 17 광역시도 + 인구 밀집/은행 밀집 시·군·구를 anchor 로 사용.
# 너무 큰 시·도(서울, 경기, 부산 등)는 자치구/시 단위로 쪼갠다.
SEARCH_ANCHORS = [
    # 서울 25 자치구
    *(f"서울 {gu}" for gu in [
        "종로구", "중구", "용산구", "성동구", "광진구", "동대문구", "중랑구",
        "성북구", "강북구", "도봉구", "노원구", "은평구", "서대문구", "마포구",
        "양천구", "강서구", "구로구", "금천구", "영등포구", "동작구", "관악구",
        "서초구", "강남구", "송파구", "강동구",
    ]),
    # 부산 16 자치구·군
    *(f"부산 {gu}" for gu in [
        "중구", "서구", "동구", "영도구", "부산진구", "동래구", "남구", "북구",
        "해운대구", "사하구", "금정구", "강서구", "연제구", "수영구", "사상구", "기장군",
    ]),
    # 인천 10
    *(f"인천 {gu}" for gu in [
        "중구", "동구", "미추홀구", "연수구", "남동구", "부평구", "계양구", "서구", "강화군", "옹진군",
    ]),
    # 대구 8
    *(f"대구 {gu}" for gu in [
        "중구", "동구", "서구", "남구", "북구", "수성구", "달서구", "달성군",
    ]),
    # 광주 5
    *(f"광주 {gu}" for gu in ["동구", "서구", "남구", "북구", "광산구"]),
    # 대전 5
    *(f"대전 {gu}" for gu in ["동구", "중구", "서구", "유성구", "대덕구"]),
    # 울산 5
    *(f"울산 {gu}" for gu in ["중구", "남구", "동구", "북구", "울주군"]),
    # 세종
    "세종",
    # 경기 — 주요 시 31
    *(f"경기 {si}" for si in [
        "수원시", "성남시", "용인시", "고양시", "화성시", "안양시", "부천시", "남양주시",
        "안산시", "평택시", "시흥시", "파주시", "의정부시", "김포시", "광주시", "광명시",
        "군포시", "오산시", "이천시", "양주시", "안성시", "구리시", "포천시", "의왕시",
        "하남시", "여주시", "양평군", "동두천시", "과천시", "가평군", "연천군",
    ]),
    # 강원
    *(f"강원 {si}" for si in [
        "춘천시", "원주시", "강릉시", "동해시", "태백시", "속초시", "삼척시",
        "홍천군", "횡성군", "영월군", "평창군", "정선군", "철원군", "화천군", "양구군", "인제군", "고성군", "양양군",
    ]),
    # 충북
    *(f"충북 {si}" for si in [
        "청주시", "충주시", "제천시", "보은군", "옥천군", "영동군", "증평군", "진천군", "괴산군", "음성군", "단양군",
    ]),
    # 충남
    *(f"충남 {si}" for si in [
        "천안시", "공주시", "보령시", "아산시", "서산시", "논산시", "계룡시", "당진시",
        "금산군", "부여군", "서천군", "청양군", "홍성군", "예산군", "태안군",
    ]),
    # 전북
    *(f"전북 {si}" for si in [
        "전주시", "군산시", "익산시", "정읍시", "남원시", "김제시",
        "완주군", "진안군", "무주군", "장수군", "임실군", "순창군", "고창군", "부안군",
    ]),
    # 전남
    *(f"전남 {si}" for si in [
        "목포시", "여수시", "순천시", "나주시", "광양시",
        "담양군", "곡성군", "구례군", "고흥군", "보성군", "화순군", "장흥군", "강진군",
        "해남군", "영암군", "무안군", "함평군", "영광군", "장성군", "완도군", "진도군", "신안군",
    ]),
    # 경북
    *(f"경북 {si}" for si in [
        "포항시", "경주시", "김천시", "안동시", "구미시", "영주시", "영천시", "상주시", "문경시", "경산시",
        "군위군", "의성군", "청송군", "영양군", "영덕군", "청도군", "고령군", "성주군", "칠곡군",
        "예천군", "봉화군", "울진군", "울릉군",
    ]),
    # 경남
    *(f"경남 {si}" for si in [
        "창원시", "진주시", "통영시", "사천시", "김해시", "밀양시", "거제시", "양산시",
        "의령군", "함안군", "창녕군", "고성군", "남해군", "하동군", "산청군", "함양군", "거창군", "합천군",
    ]),
    # 제주
    *(f"제주 {si}" for si in ["제주시", "서귀포시"]),
]


def _resolve_env(*candidates: str) -> str | None:
    for k in candidates:
        v = os.environ.get(k, "").strip()
        if v:
            return v
    return None


def _match_bank(name: str, category: str, terms: list[str]) -> bool:
    haystack = f"{name} {category}"
    for t in terms:
        if t and t in haystack:
            return True
    return False


def _is_real_branch(doc: dict) -> bool:
    name = doc.get("place_name") or ""
    cat = doc.get("category_name") or ""
    if EXCLUDE_NAME_RE.search(name):
        return False
    if EXCLUDE_CAT_RE.search(cat):
        return False
    return True


def _kakao_keyword(rest_key: str, query: str, page: int) -> dict | None:
    headers = {"Authorization": f"KakaoAK {rest_key}"}
    params = {
        "query": query,
        "page": page,
        "size": PAGE_SIZE,
        "category_group_code": "BK9",
    }
    for attempt in range(3):
        try:
            r = requests.get(KAKAO_KEYWORD_URL, headers=headers, params=params, timeout=8)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(1.0 * (attempt + 1))
                continue
            if r.status_code in (401, 403):
                print(f"  [auth] Kakao REST API 인증 실패 (status={r.status_code}) — REST 키 확인 필요", file=sys.stderr)
                return None
            print(f"  [warn] Kakao API status={r.status_code} for q={query!r}", file=sys.stderr)
            return None
        except requests.RequestException as e:
            print(f"  [warn] {e!r} — retry {attempt+1}/3", file=sys.stderr)
            time.sleep(0.5 * (attempt + 1))
    return None


def fetch_branches_for_bank(rest_key: str, bank: dict, seen: dict) -> int:
    """anchor 별 키워드 검색을 돌며 해당 은행 영업점만 누적. 반환=신규 추가 수."""
    added = 0
    for anchor in SEARCH_ANCHORS:
        query = f"{anchor} {bank['official']}"
        for page in range(1, MAX_PAGES + 1):
            payload = _kakao_keyword(rest_key, query, page)
            time.sleep(THROTTLE_SEC)
            if not payload:
                break
            docs = payload.get("documents") or []
            for d in docs:
                pid = d.get("id")
                if not pid or pid in seen:
                    continue
                if not _match_bank(d.get("place_name", ""), d.get("category_name", ""), bank["match_terms"]):
                    continue
                if not _is_real_branch(d):
                    continue
                try:
                    lng = float(d.get("x"))
                    lat = float(d.get("y"))
                except (TypeError, ValueError):
                    continue
                seen[pid] = {
                    "id": pid,
                    "bank": bank["name"],
                    "name": d.get("place_name", ""),
                    "category": d.get("category_name", ""),
                    "phone": d.get("phone", "") or "",
                    "address": d.get("address_name", "") or "",
                    "road_address": d.get("road_address_name", "") or "",
                    "lat": lat,
                    "lng": lng,
                    "place_url": d.get("place_url", "") or "",
                }
                added += 1
            if payload.get("meta", {}).get("is_end"):
                break
    return added


def write_kakao_config(js_key: str | None) -> None:
    cfg_path = DATA_DIR / "kakao_config.json"
    payload = {
        "jsKey": js_key or "",
        "updated": datetime.now(KST).isoformat(timespec="seconds"),
    }
    cfg_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if js_key:
        print(f"  [config] data/kakao_config.json 갱신 (JS key 길이={len(js_key)})")
    else:
        print("  [config] KAKAO_JS_KEY 미설정 — 프론트는 키 미설정 안내를 표시합니다.")


def main() -> int:
    rest_key = _resolve_env("KAKAO_REST_API_KEY", "KAKAO_REST_API", "KAKAO_REST_KEY", "KAKAO_API_KEY")
    js_key = _resolve_env("KAKAO_JS_KEY", "KAKAO_JAVASCRIPT_KEY", "KAKAO_MAP_JS_KEY")

    write_kakao_config(js_key)

    out_path = DATA_DIR / "kakao_branches.json"

    if not rest_key:
        print("[skip] KAKAO_REST_API_KEY 미설정 — 영업점 좌표 수집을 건너뜁니다.")
        if not out_path.exists():
            out_path.write_text(json.dumps({
                "as_of": datetime.now(KST).isoformat(timespec="seconds"),
                "source": "Kakao Local API (미수집 — REST API 키 필요)",
                "is_fallback": True,
                "branches": [],
                "summary": {},
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    all_seen: dict[str, dict] = {}
    summary = {}
    for bank in TARGET_BANKS:
        before = len(all_seen)
        added = fetch_branches_for_bank(rest_key, bank, all_seen)
        after = len(all_seen)
        bank_count = sum(1 for v in all_seen.values() if v["bank"] == bank["name"])
        summary[bank["name"]] = bank_count
        print(f"[bank] {bank['official']}: +{added} (누적 {bank_count}개)")

    branches = sorted(all_seen.values(), key=lambda x: (x["bank"], x["name"]))
    payload = {
        "as_of": datetime.now(KST).isoformat(timespec="seconds"),
        "source": "Kakao Local API · category=BK9 + 시·군·구 키워드",
        "is_fallback": False,
        "summary": summary,
        "total": len(branches),
        "branches": branches,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] {len(branches)}건 수집 → {out_path.relative_to(DATA_DIR.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

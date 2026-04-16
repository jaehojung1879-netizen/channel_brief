"""
은행연합회 보도자료 / 공시 스크래퍼
https://www.kfb.or.kr/news/press_list.php (보도자료)
"""
import requests
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
import hashlib

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

# 채널/점포/접근성 관련 키워드 - 이 단어가 제목에 있으면 우선 노출
PRIORITY_KEYWORDS = [
    "점포", "영업점", "채널", "ATM", "키오스크", "공동", "접근성",
    "고령", "장애", "디지털", "비대면", "통계", "공시",
]


def make_id(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]


def fetch_kfb_press():
    """은행연합회 보도자료 목록 파싱

    주의: 실제 사이트 구조는 변경될 수 있음. 실패 시 빈 리스트 반환.
    """
    url = "https://www.kfb.or.kr/news/press_list.php"
    items = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        # 테이블 구조 탐색 - 게시판형 사이트의 일반적 패턴
        rows = soup.select("table tr") or soup.select("ul.board_list li") or []

        for row in rows[:30]:
            text = row.get_text(" ", strip=True)
            if not text or len(text) < 10:
                continue
            # 링크 찾기
            link_tag = row.find("a")
            if not link_tag:
                continue

            title = link_tag.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            href = link_tag.get("href", "")
            if href and not href.startswith("http"):
                href = "https://www.kfb.or.kr/news/" + href.lstrip("/")

            # 날짜 추출 (YYYY-MM-DD 또는 YYYY.MM.DD)
            date_match = re.search(r"(\d{4}[-.]\d{2}[-.]\d{2})", text)
            date_str = date_match.group(1).replace(".", "-") if date_match else ""

            items.append({
                "id": make_id(href or title),
                "title": title,
                "link": href,
                "date": date_str,
                "source": "은행연합회",
                "priority": any(kw in title for kw in PRIORITY_KEYWORDS),
            })
    except Exception as e:
        print(f"[kfb] error: {e}")

    return items


def fetch_fsc_news():
    """금융위원회 보도자료 (간이 스크래핑)"""
    # 공식 사이트: https://www.fsc.go.kr/no010101
    url = "https://www.fsc.go.kr/no010101"
    items = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")

        # 보도자료 리스트 - 클래스 명은 사이트 개편 시 바뀔 수 있음
        cards = soup.select(".board_list li") or soup.select("ul.list li") or soup.select("tbody tr")

        for c in cards[:20]:
            link_tag = c.find("a")
            if not link_tag:
                continue
            title = link_tag.get_text(strip=True)
            if len(title) < 5:
                continue
            href = link_tag.get("href", "")
            if href and not href.startswith("http"):
                href = "https://www.fsc.go.kr" + ("/" if not href.startswith("/") else "") + href

            text = c.get_text(" ", strip=True)
            date_match = re.search(r"(\d{4}[-.]\d{2}[-.]\d{2})", text)
            date_str = date_match.group(1).replace(".", "-") if date_match else ""

            items.append({
                "id": make_id(href or title),
                "title": title,
                "link": href,
                "date": date_str,
                "source": "금융위원회",
                "priority": any(kw in title for kw in PRIORITY_KEYWORDS),
            })
    except Exception as e:
        print(f"[fsc] error: {e}")

    return items


def main():
    print(f"[disclosure] start at {datetime.now(KST).isoformat()}")

    kfb = fetch_kfb_press()
    fsc = fetch_fsc_news()

    # 우선순위 항목 먼저, 그 다음 날짜 최신순
    def sort_key(x):
        return (not x.get("priority", False), -int(x.get("date", "0000-00-00").replace("-", "") or 0))

    kfb.sort(key=sort_key)
    fsc.sort(key=sort_key)

    output = {
        "updated_at": datetime.now(KST).isoformat(),
        "kfb": kfb[:15],
        "fsc": fsc[:15],
    }

    out_path = DATA_DIR / "disclosure.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[disclosure] saved: {out_path} (kfb={len(kfb)}, fsc={len(fsc)})")


if __name__ == "__main__":
    main()

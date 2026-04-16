"""
LH / 국토교통부 개발 공시 + 시중은행 점포 수 추이
개발 공시는 구글뉴스 RSS로 우회 (공식 API 없음)
점포 수는 은행연합회 통계 → 수동 업데이트 파일 기반
"""
import feedparser
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote
import time
import hashlib

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

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


def load_branch_stats():
    """시중은행 점포 수 추이 - 수동 유지 데이터 파일
    은행연합회에서 분기마다 공표하므로 자동화 대신 stat_data.json을 분기별 업데이트"""
    stat_file = DATA_DIR / "branch_stats_manual.json"
    if not stat_file.exists():
        # 초기 샘플 데이터
        default = {
            "as_of": "2025-12-31",
            "source": "은행연합회 은행업무 통계",
            "banks": [
                {"name": "KB국민", "count": 803, "delta_qoq": -12},
                {"name": "신한", "count": 747, "delta_qoq": -8},
                {"name": "하나", "count": 621, "delta_qoq": -9},
                {"name": "우리", "count": 676, "delta_qoq": -11},
                {"name": "NH농협", "count": 1080, "delta_qoq": -2},
            ],
            "total": 3927,
            "total_delta_qoq": -42,
        }
        stat_file.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
        return default

    return json.loads(stat_file.read_text(encoding="utf-8"))


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

    stats_output = {
        "updated_at": datetime.now(KST).isoformat(),
        **stats,
    }
    (DATA_DIR / "branch_stats.json").write_text(
        json.dumps(stats_output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[stats] saved: {stats['as_of']}")


if __name__ == "__main__":
    main()

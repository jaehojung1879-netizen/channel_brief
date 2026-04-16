# 채널전략부 Daily Brief

영업점·채널 관련 뉴스, 은행연합회·금융위 공시, 개발 공시, 점포 추이, 채널 이슈, 해외 벤치마크를 **평일 매일 오전 7시(KST) 자동 취합**하여 공개 URL로 제공하는 정적 대시보드.

## 구조

```
channel-brief/
├── index.html                          # 대시보드 본체 (JSON fetch)
├── requirements.txt                    # Python 의존성
├── .github/workflows/daily-update.yml  # 자동 실행 워크플로
├── scripts/
│   ├── fetch_news.py                   # 구글뉴스 RSS 기사 수집
│   ├── fetch_disclosure.py             # 은행연합회/금융위 공시
│   ├── fetch_dev_and_stats.py          # 개발 공시 + 점포 통계
│   └── fetch_issues_global.py          # 채널 이슈 + 해외 사례
└── data/
    ├── news.json                       # 자동 생성
    ├── disclosure.json                 # 자동 생성
    ├── development.json                # 자동 생성
    ├── branch_stats.json               # 자동 생성 (manual 기반)
    ├── branch_stats_manual.json        # **분기별 수동 업데이트 필요**
    ├── issues.json                     # 자동 생성
    └── global.json                     # 자동 생성
```

---

## 배포 5단계

### 1. GitHub 계정 준비
- https://github.com 에서 계정 생성 (이미 있으면 skip)
- **주의**: 회사 이메일이 아닌 개인 이메일 권장 (부서 공유용이되 소유권은 개인)

### 2. 새 레포지토리 생성
- Repository name: `channel-brief` (아무거나 가능)
- Public 선택 (GitHub Pages 무료 플랜 사용)
- "Add a README" 체크 해제 (이미 있음)

### 3. 로컬 파일 업로드
방법 A — 웹 UI (간단):
- 레포 페이지 → "uploading an existing file" → 압축 해제된 전체 폴더 드래그 → Commit

방법 B — Git CLI:
```bash
cd channel-brief
git init
git add .
git commit -m "initial: channel brief dashboard"
git branch -M main
git remote add origin https://github.com/<사용자명>/channel-brief.git
git push -u origin main
```

### 4. GitHub Pages 활성화
- 레포 → **Settings** → **Pages**
- Source: `Deploy from a branch`
- Branch: `main` / `/ (root)`
- Save 클릭
- 약 1분 후 `https://<사용자명>.github.io/channel-brief/` 접속 가능

### 5. Actions 권한 확인
- 레포 → **Settings** → **Actions** → **General**
- "Workflow permissions" → **Read and write permissions** 선택
- Save

### 6. 첫 실행 (수동 트리거)
- 레포 → **Actions** 탭 → `Daily Channel Brief Update` → **Run workflow** 클릭
- 3-5분 후 data/ 폴더에 JSON 파일들이 자동 생성·커밋됨
- 대시보드 URL 새로고침 → 실제 데이터 표시

---

## 운영 체크리스트

### 자동화되는 것 (매일 오전 7시 KST)
- 구글뉴스 RSS로 영업점/채널/정책 관련 기사 최대 20건
- 은행연합회 보도자료 목록
- 금융위 보도자료 목록
- LH·국토부 관련 개발 공시 (구글뉴스 경유)
- 채널 이슈 (장차법·ATM·키오스크·고령자금융)
- 해외 리테일 뱅킹 혁신 사례 (영문)

### 수동 관리가 필요한 것
| 항목 | 파일 | 주기 |
|------|------|------|
| 시중은행 점포 수 통계 | `data/branch_stats_manual.json` | 분기별 (은행연합회 통계 발표 후) |
| 검색 키워드 조정 | `scripts/*.py` 상단 상수 | 필요 시 |
| 섹션 추가/삭제 | `index.html` | 필요 시 |

### 모니터링
- Actions 탭에서 빨간 X 표시가 뜨면 실행 실패
- 일부 스크래퍼가 실패해도 `continue-on-error: true` 설정으로 다른 것은 정상 실행됨
- 은행연합회·금융위 사이트 개편 시 `fetch_disclosure.py`의 CSS 선택자 수정 필요

---

## 예상 비용

| 항목 | 비용 |
|------|------|
| GitHub Pages 호스팅 | 무료 |
| GitHub Actions (월 2,000분 한도) | 무료 (본 워크플로는 월 약 100분 소비) |
| 도메인 (선택) | `.io`, `.com` 유료 / 기본 `.github.io` 무료 |

---

## 주의사항

### 1. 구글뉴스 RSS 사용 정책
- RSS는 공식 제공 경로이므로 ToS 이슈 없음
- 단, 과도한 빈도(분 단위) 요청 시 차단 가능 → 본 설정(일 1회)은 안전 수준

### 2. 은행연합회·금융위 사이트
- 공공 사이트이며 robots.txt 허용 범위 내에서 작동
- 사이트 개편 시 선택자 수정 필요 → 실패 시 해당 섹션만 빈 상태로 표시

### 3. 보안
- **절대 커밋하지 말 것**: API 키, 내부 자료, 신한은행 전용 문서
- 본 레포는 공개이므로 모든 파일이 외부 노출됨
- 내부 데이터 결합이 필요하면 별도 Private 레포 + 수동 배포 방식 검토

### 4. 저작권
- 기사 제목·요약·링크만 표시 (전문 미표시)
- 클릭 시 원문 사이트로 이동 → 저작권 이슈 없음

---

## 커스터마이징 팁

### 키워드 추가
`scripts/fetch_news.py` 의 `KEYWORDS` 딕셔너리 수정.

### 색상 테마 변경
`index.html` 상단 `:root` CSS 변수:
- `--accent`: 메인 액센트 컬러
- `--bg`: 배경색
- `--ink`: 본문 텍스트

### 섹션 추가
1. `scripts/` 에 새 Python 파일 추가 → `data/xxx.json` 생성
2. `.github/workflows/daily-update.yml` 에 단계 추가
3. `index.html` 에 섹션 HTML + JS 렌더 함수 추가

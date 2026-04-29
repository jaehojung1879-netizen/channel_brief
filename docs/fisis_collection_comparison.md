# FISIS 수집 로직 비교 (은행별/지역별 점포 vs 자동화기기)

이 문서는 `scripts/fetch_dev_and_stats.py` 기준으로, 세 수집 로직을 같은 시각으로 비교하기 위한 운영 참고서입니다.

## 1) 진입 함수

- 은행별 점포: `fisis_build_branch_stats(codes)`
- 지역별 점포: `fisis_build_regional_stats(codes)`
- 자동화기기: `fisis_build_atm_stats(codes)`

공통으로 `codes`에서 `list_no_*`, `bank_finance_codes`를 사용합니다.

## 2) 공통 수집 패턴

1. `listNo`가 있는지 확인한다.
2. 은행별 `financeCd`로 먼저 조회한다. (`_fisis_fetch_info`)
3. 일부/전체 은행 누락 시 `financeCd=""` 전체조회 fallback을 시도한다.
4. 최신 기준월(`latest_ym`) 기반으로 가공한다.

## 3) 함수별 차이

| 구분 | 은행별 점포 | 지역별 점포 | 자동화기기 |
|---|---|---|---|
| 필수 조건 | `list_no_branch` + 은행코드 4개 | `list_no_regional` + 은행코드 4개 | `list_no_atm`만 필수 |
| 1차 조회 | 은행별 `financeCd` | 은행별 `financeCd` + `account_cd=A11/A12` 보조 | 은행별 `financeCd` |
| fallback | 없음(현재는 4개 모두 필요) | 있음(`financeCd=""` 전체행 파싱) | 있음(`financeCd=""` 전체행 파싱) |
| 결과 구조 | 은행별 count/history | 지역→은행→count/branches/sub_offices/history | 은행별 ATM 항목(code)/합계/history |
| 추가 디버그 | 로그 중심 | 로그 중심 | `atm_devices.debug` 포함 |

## 4) 자동화기기 디버그 필드

`fisis_build_atm_stats` 결과는 다음 디버그 정보를 포함합니다.

- `debug.list_no_atm`: 사용한 ATM 통계표 번호
- `debug.finance_cd_targets`: 은행별 1차 조회 대상 개수
- `debug.per_bank_query_success`: 은행별 조회 row 수
- `debug.fallback_all_rows_used`: 전체조회 fallback 사용 여부
- `debug.fallback_all_rows`: fallback 전체 row 수
- `debug.resolved_banks`: 실데이터 확보 은행
- `debug.missing_banks`: 최종 미확보 은행

## 5) 빠른 점검 방법

1. 수집 실행 후 `data/branch_stats.json`에서 `atm_devices.debug`를 확인한다.
2. `missing_banks`가 비어있지 않으면 FISIS 응답 스키마/은행명 매핑을 확인한다.
3. `fallback_all_rows`가 0이면 해당 `listNo` 자체 유효성 또는 API key/기간 파라미터를 점검한다.

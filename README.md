# gefs-collector

KMTNet 시잉 예보용 GEFS 앙상블 무인 수집기 (GitHub Actions).
NOAA GEFS 공공데이터(AWS Open Data)에서 Paranal 지점 예보 피처를 추출해 `data/gefs_ens.jsonl`에 누적.
관측 라벨은 여기 없음 — 본 레포는 공공 기상데이터 추출값만 저장.

## 저장 구조 (v2, 2026-07-25)
- `data/v2/{YYYY-MM}_m{M}.jsonl.gz` — 월별·멤버별 gzip (행=사이트별: paranal/ctio/saao/sso, 31층 t/u/v 배열 + 지표 + tcdc/pwat[컨트롤])
- `data/legacy_v1/` — 구스펙(3층·일단위·파라날) 보존분. 신규 분석은 v2만 사용 권장
- 백필 완료 후 1회 컴팩션(이력 스쿼시) 예정 — 클론 크기 관리

## 이력 컴팩션 (2026-08-09) — 한 번 했고, 왜·어떻게

**왜**: 수집기는 `.jsonl.gz` 에 20분마다 덧붙이는데, **gzip 은 한 줄만 붙어도 파일 전체가 바뀐다.**
git 이 차이를 저장할 수 없어 커밋마다 전체 사본이 쌓였다. 커밋 3,888개(머지 949개) →
GitHub 889 MB. 그중 현재 데이터가 613 MB 이고 나머지 약 300 MB 가 옛 버전이었다.

**잃는 것이 없음을 먼저 확인했다**: 수집기는 append 전용이라 옛 버전은 현재 파일의 앞부분이다.
표본 3개(8·4·9회 커밋된 파일)에서 「옛것이 현재의 접두」가 전부 참이었다.

**전체 이력은 오프라인에 남겼다**: `E:\git_backup_20260809\kmtnet-gefs-collector.git`
(`git clone --mirror`, 커밋 3,888개·HEAD `5486749c` 일치 검증). 코드 이력이 필요하면 거기서 본다.

**절차** (다음에 또 하게 되면 이대로):

```bash
# 1) 백업 — 되돌릴 길을 먼저 만든다
git clone --mirror . E:/git_backup_YYYYMMDD/kmtnet-gefs-collector.git

# 2) 크론 정지 — force-push 중에 워커가 푸시하면 그 회차가 깨진다
for w in backfill_v2 backfill_v2x backfill_v2y watchdog; do gh workflow disable $w.yml; done
gh run list --status in_progress          # 도는 것이 없어야 한다

# 3) ★ 작업트리를 쓰지 말 것 — commit-tree 로 HEAD 의 트리를 그대로 가리킨다
NEW=$(git commit-tree "HEAD^{tree}" -m "...")
git diff HEAD "$NEW" --stat               # 아무것도 안 나와야 한다
git update-ref refs/heads/master "$NEW"
git push --force origin master

# 4) 원격에서 새로 클론해 검증 (파일 수·최신 cycle)
# 5) 로컬 정리 → 크론 복구 → 시험 실행 1회
git reflog expire --expire=now --all && git gc --prune=now --aggressive
for w in backfill_v2 backfill_v2x backfill_v2y watchdog; do gh workflow enable $w.yml; done
```

⚠ **3번이 핵심이다.** 창고 `build.py --prune` 이 로컬 `data/` 를 지워 두므로, 작업트리에서
`git add -A` 로 스쿼시하면 **데이터 1,420개가 전부 삭제된 커밋**이 만들어진다.
2026-08-09 에 실제로 작업트리 데이터가 0개인 상태였다 — `commit-tree` 로 우회했다.

**결과**: 로컬 `.git` 1,383 → **588 MB**. GitHub 쪽은 서버 gc 가 돌아야 반영된다(지연 있음).

**구조적 한계**: 컴팩션은 시간을 벌 뿐이다. 수집이 계속되면 다시 쌓인다.
근본 해법은 **데이터를 git 밖으로 빼는 것** — 창고 parquet + `E:\weather_warehouse_mirror` 가
이미 정본이므로(ERA5 에서 이미 내린 결정과 같다), GEFS parquet 이 무손실임을 검증하면
git 은 코드만 두고 5 MB 로 줄일 수 있다. 그 검증이 선행 조건이다.

## 비용 모델 — 무엇이 비싸고 무엇이 공짜인가 (2026-08-27 · F-168)

⚠ **격자(지점 수)는 비용이 아니다.** 수집 단위는 **변수 × 층**이다:

```python
dss = H.xarray(search, remove_grib=True)      # search = 변수·층 → 여기서 받는다
pts = {s: ds.sel(latitude=..., method="nearest") for s in SITES}   # 점 뽑기는 인덱싱, 공짜
```

같은 파일에서 점을 1개 뽑든 9개 뽑든 **다운로드량은 같다.**
이미 4사이트를 한 번에 뽑고 있고, 그래서 네 지점의 행 수가 487,067 로 정확히 같다.

| 늘리는 것 | 추가 다운로드 | 비고 |
|---|---|---|
| 지점 1 → 3×3 (사이트당 9점) | **0%** | 저장량만 9배 |
| 사이트 4 → 8 | **0%** | 같은 필드에서 더 뽑을 뿐 |
| **멤버 5 → 31** | **6.2배** | ← 진짜 비용 |
| **변수 3종(t·u·v) → 5종(+HGT·SPFH)** | **약 1.7배** | ← 진짜 비용 |
| 리드 7종 → 14종 | 2배 | ← 진짜 비용 |

**「N배」라고 쓰기 전에 그 배수를 만드는 루프를 코드에서 찾을 것.** 못 찾으면 비용이 아니다.

### 재산정한 재수집 사양 (2026-08-27)

| 안 | 내용 | 활동일 추정 |
|---|---|---|
| A | 3×3 격자 + 5멤버 + 현재 변수 | **6일** (원래와 동일) |
| B | A + **HGT·SPFH 를 전 리드에** | **10일** |
| C | 3×3 + 31멤버 | 5주 |

기준: 원래 수집이 2026-08-09~15 구간 활동일 6일 · 1,407 커밋 (F-167).

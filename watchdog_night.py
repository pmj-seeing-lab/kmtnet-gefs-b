# -*- coding: utf-8 -*-
"""GEFS night 수집을 3시간마다 스스로 점검한다 (2026-09-01 신설).

왜 만들었나
    2026-09-01 에 사람이 손으로 캐서야 세 가지가 드러났다.

      ① 계정 B 가 20잡을 한 번에 못 받아 **17잡 + 3잡 두 물결**로 돌았다.
         뒤 5시간 동안 17슬롯이 놀아 평균 동시성이 10 이었다 (계정 A 는 20).
      ② 구간 경계가 **연도 중간**을 지나 두 잡이 같은 `{연도}_m{멤버}.jsonl.gz` 에
         동시에 append 했다. git 병합이 한쪽을 버린다 (`Main/FAILURES.md` F-200).
      ③ 멤버 5·8 이 한 구간에서 **5시간 동안 성공 0건** (`ok 0 fail 5,617`)이었다.
         같은 시간대 다른 멤버는 실패 0% 였다.

    셋 다 **종료코드는 0** 이었고 워크플로는 초록색이었다. 사람이 2~3시간마다
    들여다봐야 보이는 상태였고, 그건 지속되지 않는다.

무엇을 어떻게 알리나 — **메일이 아니다**
    사용자 지적: 「메일이면 내가 너한테 다시 보내 줘야하잖어」. 맞다.
    그래서 판정을 **레포 안에 커밋**한다. 사람이 중계하지 않아도 에이전트가
    `gh api repos/{owner}/{repo}/contents/_watchdog/status.md` 로 바로 읽는다.

      `_watchdog/status.md`      사람·에이전트가 읽는 최신 판정 (매번 덮어쓴다)
      `_watchdog/history.jsonl`  한 실행에 한 줄 (추세를 보려고 쌓는다)

    이상이 있으면 **종료코드 1** 로 끝내 실행 목록에 빨간 X 를 남긴다.
    다만 그건 곁가지다 — 정본은 위 상태 파일이다.

무엇을 보나 (자료 파일은 안 건드린다 — 커밋 메시지 + Actions API + 워크플로 파일뿐)
    A 멤버별 실패율·속도    최근 커밋의 `night m{M} ok N fail M (R/h)` 를 센다
    B 잡이 한 물결인가      최신 night 실행의 잡 시작시각이 전부 같은 분인가
    C 연도 파일 충돌        night.yml 구간을 펼쳐 두 구간에 속한 연도가 있나 (①② 회귀 가드)
    D 최근 24시간 실행      성공이 하나라도 있나
    E 수집이 살아 있나      가장 최근 `night ` 커밋이 얼마나 오래됐나
    F 진행률이 늘고 있나    잡 로그의 `할 일 N (이미 N)` 을 읽어 멤버별 %,
                           그리고 **지난 판정 대비 안 늘었으면** 이상

쓰는 법
    python -X utf8 watchdog_night.py            # 점검하고 상태 파일을 쓴다 (Actions 가 부른다)
    python -X utf8 watchdog_night.py --dry      # 파일을 쓰지 않고 판정만 찍는다
"""
import collections
import io
import json
import os
import re
import subprocess
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "_watchdog")

REPO = os.environ.get("GITHUB_REPOSITORY", "")
WF_NAME = "GEFS night"
NIGHT_YML = os.path.join(HERE, ".github", "workflows", "night.yml")

# ── 판정 문턱 — 왜 이 값인지 같이 적는다 ─────────────────────────────
FAIL_RATE_BAD = 0.20      # 실패율 20% 넘으면 이상. 2026-09-01 실측 m8 48.3% · m5 26.2%
SLOW_FRAC = 0.40          # 중앙 속도의 40% 미만이면 이상. 성공 0건 잡은 1~28/h 였다 (정상 ~590)
# 마지막 night 커밋이 이보다 오래되면 이상.
#   ★ 90 → 480분 (2026-09-01). 90분으로 뒀더니 첫 자동 판정이 바로 울렸는데,
#     원인이 고장이 아니라 **정상 간격**이었다 — 세션이 할 일을 다 끝내고 일찍
#     닫힌 뒤, GitHub 이 다음 cron(6시간)을 늦게 투입한 것 (22:46 성공 종료 →
#     다음 실행 생성 04:59, 실측). 수집이 차 갈수록 세션이 짧아져 이 간격은
#     오히려 늘어난다. 진짜 정지는 검사 D(최근 실행에 성공·진행중 없음)가 잡는다.
#     480 = cron 6시간 + 지연 여유 2시간.
COMMIT_STALE_MIN = 480
LOOKBACK_H = 6            # 실패율을 셀 창. 한 세션(5시간)보다 조금 넓게

#  `night m4 ok 2160 fail 0 (908/h)` 와 `night m4/2020-01 ok ...` 를 둘 다 받는다.
#  (워커가 구간을 적기 시작해도 이 규칙이 그대로 먹는다.)
CM = re.compile(r"night m(\d+)(?:/[\w-]+)? ok (\d+) fail (\d+) \((\d+)/h\)")

#  워커가 시작할 때 찍는 진행 줄 (`worker_night.py` 332~334행):
#      [m0] 리드 28개(…) / 57개(…) · 사이클 3,148 · 할 일 12,345 (이미 138,178) · 워커 6
#  `이미` = `done_keys(M)` — 그 멤버가 **전 연도에서** 이미 받은 (사이클, 리드) 수.
#          구간과 무관하므로 같은 멤버의 잡 4개가 같은 값을 찍는다.
#  `할 일` = **그 구간에** 남은 수. 구간마다 다르므로 4개를 더해야 멤버 잔량이다.
#  ⇒ 분모 = 이미 + Σ(구간별 할 일)
#  ⚠ 커밋 메시지의 ok/fail 로는 이걸 못 낸다 — 세션 누적값이고 구간이 안 적혀 섞인다
#    (검사 A 의 머리말 참조). 그래서 진행률만 잡 로그를 읽는다.
PG = re.compile(r"\[m(\d+)\].*?할 일 ([\d,]+) \(이미 ([\d,]+)\)")
PROGRESS_STALL = True     # 지난 판정보다 안 늘면 이상으로 잡는다
# 이 비율을 넘으면 **완주로 본다.** 그러면 「커밋이 없다」·「커밋이 오래됐다」를
# 이상으로 잡지 않는다 — 받을 것이 없으니 커밋도 없는 게 맞다.
#   ★ 2026-09-03 에 필요해졌다. 계정 A 가 멤버 0~4 를 100.0% 끝내자
#     감시가 「최근 6시간에 커밋이 하나도 없다」+「마지막 커밋 1,549분 전」으로
#     이상 2건을 울렸다. 둘 다 사실이지만 **고장이 아니라 완주**였다.
#   왜 100% 가 아니라 99.9% 인가: 멤버당 36~69 단위가 영구히 남는다 —
#     NOAA 아카이브에 그 사이클이 아예 없다. 100% 를 기다리면 영원히 안 온다.
PROGRESS_DONE_PCT = 99.9


def sh(*a):
    r = subprocess.run(a, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return r.returncode, (r.stdout or ""), (r.stderr or "")


def gh_json(path):
    """`gh api` 로 JSON 을 받는다. 실패하면 None — 판정을 멈추지 않는다."""
    rc, out, err = sh("gh", "api", path)
    if rc != 0:
        return None
    try:
        return json.loads(out)
    except Exception:
        return None


# ── A. 멤버별 실패율·속도 ────────────────────────────────────────────
def check_members(bad, info):
    """⚠ **합계를 내지 않는다.** 두 가지 이유로 합계가 거짓말을 한다.

       ① 커밋 메시지의 `ok`·`fail` 은 **그 세션의 누적값**이다 (COMMIT_EVERY=40 마다 찍는다).
       ② 한 멤버에 잡이 **여러 개**(구간마다 하나) 돌고 **커밋 메시지에 구간이 안 적힌다.**
          그래서 여러 잡의 누적값이 한 줄기로 섞인다.

       2026-09-01 에 이걸 합쳐서 「m3 실패율 25.6%」라는 **없는 이상**을 만들어 냈다.
       실제로는 실패 0이었다.

       그래서 **커밋 하나 안의 비율**만 본다 — `fail / (ok + fail)` 는 같은 세션의
       같은 순간 값이므로 섞임과 무관하다. 시도가 충분히 쌓인 커밋만 후보로 삼는다
       (잡 시작 직후의 `ok 0 fail 0` 같은 잡음을 뺀다).

       그중 **`ok` 가 가장 큰 커밋**(= 세션 끝에 가장 가까운 지점)의 비율로 판정한다.
       ⚠ 예전에는 **가장 나쁜 비율**로 판정했는데 그게 오탐을 낳았다 —
         `fail` 은 «없는 파일» 몇십 개로 고정되고 `ok` 만 오르므로 비율이 세션 내내
         떨어진다. 초반 `ok 160 fail 40`(20%)이 잡혀 정상 세션마다 경보가 떴다.

       (워커가 `night m{M}/{구간} ...` 로 구간을 적기 시작하면 합계도 정확해진다.
        지금 규칙은 구간 표시가 있든 없든 똑같이 동작한다.)"""
    MIN_TRIES = 200           # 이만큼은 시도한 커밋만 비율 판정에 쓴다
    # ★ 2026-09-02 — **「최악 비율」로 판정하던 것을 「세션이 익은 시점의 비율」로 바꿨다.**
    #   왜: `fail` 은 «없는 파일» 몇십 개로 **고정**되는데 `ok` 는 계속 오른다.
    #   그래서 한 세션 안에서 비율이 계속 **떨어진다** —
    #       ok 160 fail 40 → 20%   ...   ok 2000 fail 40 → 2%
    #   최악(=초반) 비율을 잡으면 정상 세션마다 경보가 뜬다. 실제로 2026-09-02 에
    #   A m1 34.5% · B m5 20.5% · m6 21.5% 로 떴는데, 같은 세션의 종료 커밋은
    #   `fail 0` 이었다. **늘 울리는 경보는 진짜 정지를 가린다** (F-201 이 그렇게 숨었다).
    #
    #   그래서 멤버별로 **`ok` 가 가장 큰 커밋**(= 그 잡의 세션 끝에 가장 가까운 지점)의
    #   비율으로 판정한다. 「최악 비율」은 참고로 계속 적지만 경보에 쓰지 않는다.
    #   ⚠ 죽은 잡(`ok 0 fail 5,617`)은 `ok` 가 안 커서 이 규칙에 안 걸린다 —
    #     그건 아래 「성공 0건」 검사가 따로 잡는다. 두 검사는 겹치지 않는다.
    since = f"{LOOKBACK_H} hours ago"
    rc, out, _ = sh("git", "log", f"--since={since}", "--format=%ct%x09%s", "-4000")
    seq = collections.defaultdict(list)          # 멤버 -> [(ok, fail, rate)]
    for line in out.splitlines():
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        m = CM.search(parts[1])
        if not m:
            continue
        seq[int(m.group(1))].append(
            (int(m.group(2)), int(m.group(3)), int(m.group(4))))
    if not seq:
        if _is_done(info):
            info["커밋없음_사유"] = (
                f"완주 {info['진행률']['합계비율']}% — 받을 것이 없으니 커밋도 없다. "
                f"이상으로 잡지 않는다.")
        else:
            bad.append(f"최근 {LOOKBACK_H}시간에 `night m.. ok .. fail ..` 커밋이 하나도 없다 "
                       f"— 수집이 안 돌거나 커밋 형식이 바뀌었다")
        return {}

    def med_of(v):
        v = sorted(v)
        return v[len(v) // 2] if v else 0.0

    rate_med_all = med_of([med_of([r for _, _, r in rows]) for rows in seq.values()])
    out_rows = {}
    for mem in sorted(seq):
        rows = seq[mem]
        usable = [(o, f) for o, f, _ in rows if o + f >= MIN_TRIES]
        ratios = [f / (o + f) for o, f in usable]
        zero_ok = [(o, f) for o, f in usable if o == 0]
        rate_med = med_of([r for _, _, r in rows])
        worst = max(ratios) if ratios else 0.0
        # 판정에 쓰는 값 — `ok` 가 가장 큰 커밋(세션 끝에 가장 가까운 지점)의 비율
        mature = max((x for x in usable if x[0] > 0), key=lambda x: x[0], default=None)
        final = (mature[1] / (mature[0] + mature[1])) if mature else 0.0
        out_rows[mem] = {"커밋수": len(rows), "판정에쓴커밋": len(usable),
                         "판정실패율": round(final, 4),
                         "판정근거_ok": mature[0] if mature else 0,
                         "판정근거_fail": mature[1] if mature else 0,
                         "최악실패율_참고": round(worst, 4),
                         "중앙실패율": round(med_of(ratios), 4),
                         "중앙속도": round(rate_med, 1),
                         "성공0건커밋": len(zero_ok)}
        if zero_ok:
            mx = max(f for _, f in zero_ok)
            bad.append(f"m{mem}: **성공 0건인 커밋 {len(zero_ok)}개** (최대 실패 {mx:,}) "
                       f"— 2026-09-01 의 m5·m8 과 같은 증상이다")
        elif final > FAIL_RATE_BAD:
            bad.append(f"m{mem}: 세션이 익은 지점의 실패율 {100*final:.1f}% "
                       f"(ok {mature[0]:,} fail {mature[1]:,}) "
                       f"— 문턱 {100*FAIL_RATE_BAD:.0f}% 초과")
        if rate_med_all > 0 and rate_med < rate_med_all * SLOW_FRAC:
            bad.append(f"m{mem}: 중앙 {rate_med:.0f}단위/시간 — 전체 중앙 "
                       f"{rate_med_all:.0f} 의 {100*rate_med/rate_med_all:.0f}% 밖에 안 된다")
    info["전체중앙속도"] = round(rate_med_all, 1)
    info["판정규칙"] = (f"멤버별로 **ok 가 가장 큰 커밋**(세션 끝에 가장 가까운 지점)의 "
                        f"fail/(ok+fail) 로 판정한다. 합계는 세션 누적값이 여러 잡에서 "
                        f"섞여 못 쓰고, 「최악 비율」은 fail 이 고정된 채 ok 만 오르는 "
                        f"구조상 세션 초반을 잡아 늘 울린다 (2026-09-02 오탐). "
                        f"시도 {MIN_TRIES}건 이상인 커밋만 후보.")
    return out_rows


# ── B. 잡이 한 물결인가 ─────────────────────────────────────────────
def check_wave(bad, info):
    if not REPO:
        info["물결"] = "GITHUB_REPOSITORY 가 없어 못 봤다"
        return
    # ⚠ `actions/runs` 는 **모든 워크플로**를 섞어 준다. 이 저장소는 워크플로가 9개라
    #   최근 20건에 night 이 안 들어올 수 있다 (처음 구현이 그래서 「night 이 없다」고
    #   거짓 경보를 냈다). 워크플로 파일을 **직접 지목**한다.
    runs = gh_json(f"repos/{REPO}/actions/workflows/night.yml/runs?per_page=12")
    if not runs:
        info["물결"] = "night.yml 실행 목록을 못 받았다"
        return
    night = runs.get("workflow_runs", [])
    if not night:
        bad.append("night.yml 의 실행 이력이 비어 있다")
        return
    r0 = night[0]
    jobs = gh_json(f"repos/{REPO}/actions/runs/{r0['id']}/jobs?per_page=100")
    if not jobs:
        info["물결"] = f"실행 #{r0['id']} 의 잡을 못 받았다"
        return
    js = jobs.get("jobs", [])
    starts = collections.Counter(str(j.get("started_at"))[:16] for j in js if j.get("started_at"))
    info["최신실행"] = {"id": r0["id"], "상태": r0.get("status"),
                        "결론": r0.get("conclusion"), "생성": r0.get("created_at"),
                        "잡수": len(js), "시작시각별": dict(sorted(starts.items()))}
    if len(js) == 0:
        # 잡 0개는 **정상**이다 — 동시성 그룹이 중복 대기를 버린 것이고 손실이 없다.
        info["물결"] = "잡 0개 — 중복 대기가 버려진 실행이다 (정상)"
        return
    if len(starts) > 1:
        big = max(starts.values())
        tail = len(js) - big
        bad.append(f"잡이 **{len(starts)}개 물결**로 갈렸다 — {dict(sorted(starts.items()))}. "
                   f"뒤쪽 {tail}개가 앞 물결을 기다리는 동안 {big}슬롯이 놀게 된다. "
                   f"행렬을 {big}잡 이하로 줄일 것 (2026-09-01 에 계정 B 가 17+3 이었다)")
    else:
        info["물결"] = f"한 물결 — 잡 {len(js)}개가 전부 {list(starts)[0]} 에 시작"


# ── C. 연도 파일 충돌 (F-200 회귀 가드) ──────────────────────────────
def check_spans(bad, info):
    if not os.path.exists(NIGHT_YML):
        info["구간"] = "night.yml 을 못 찾았다"
        return
    txt = io.open(NIGHT_YML, encoding="utf-8").read()
    spans = re.findall(r'start:\s*"(\d{4})-\d{2}-\d{2}".*?end:\s*"(\d{4})-\d{2}-\d{2}"', txt)
    mem = re.search(r"member:\s*\[([0-9,\s]+)\]", txt)
    nmem = len([x for x in mem.group(1).split(",") if x.strip()]) if mem else 0
    own = collections.defaultdict(list)
    for i, (a, b) in enumerate(spans):
        for y in range(int(a), int(b) + 1):
            own[y].append(i)
    clash = {y: v for y, v in own.items() if len(v) > 1}
    info["구간"] = {"구간수": len(spans), "멤버수": nmem, "잡수": nmem * len(spans),
                    "연도별소유": {str(y): v for y, v in sorted(own.items())}}
    if clash:
        bad.append(f"**연도 파일 충돌** — {sorted(clash)} 를 두 구간이 같이 쓴다. "
                   f"파일이 `{{연도}}_m{{멤버}}.jsonl.gz` 라 두 잡이 같은 파일에 append 하고 "
                   f"git 병합이 한쪽을 버린다 (FAILURES F-200). 경계를 연말로 맞출 것")


# ── D·E. 실행 상태와 커밋 신선도 ─────────────────────────────────────
def _is_done(info):
    """진행률이 완주선을 넘었나. 진행률을 못 읽었으면 **False** — 모르면 울린다."""
    pg = info.get("진행률")
    if not isinstance(pg, dict) or pg.get("로그못읽은잡"):
        return False
    r = pg.get("합계비율")
    return isinstance(r, (int, float)) and r >= PROGRESS_DONE_PCT


# ── F. 진행률 ────────────────────────────────────────────────────────
def _last_progress():
    """지난 판정의 멤버별 `받음` 을 `history.jsonl` 마지막 줄에서 읽는다."""
    p = os.path.join(OUT, "history.jsonl")
    if not os.path.exists(p):
        return {}, None
    last = None
    for line in io.open(p, encoding="utf-8"):
        if line.strip():
            last = line
    if not last:
        return {}, None
    try:
        rec = json.loads(last)
    except Exception:
        return {}, None
    pg = (rec.get("정보") or {}).get("진행률") or {}
    return {k: v.get("받음") for k, v in (pg.get("멤버") or {}).items()}, rec.get("t")


def check_progress(bad, info):
    """잡 로그에서 멤버별 진행률을 읽는다.

    ⚠ `이미` 는 잡이 **시작한 시점**의 값이다 — 실행 중이면 실제로는 더 받았다.
      그래서 「안 늘었다」 판정은 **실행이 새로 시작한 뒤**에만 뜻이 있다.
      같은 실행을 두 번 읽으면 당연히 같은 값이라, 실행 id 가 바뀌었을 때만 비교한다.
    """
    if not REPO:
        info["진행률"] = "GITHUB_REPOSITORY 가 없어 못 봤다"
        return
    runs = gh_json(f"repos/{REPO}/actions/workflows/night.yml/runs?per_page=6")
    if not runs:
        info["진행률"] = "실행 목록을 못 받았다"
        return
    rid = created = jobs = None
    for r in runs.get("workflow_runs", []):
        jj = gh_json(f"repos/{REPO}/actions/runs/{r['id']}/jobs?per_page=40")
        if not jj:
            continue
        js = [j for j in jj.get("jobs", []) if j.get("started_at")]
        if js:
            rid, created, jobs = r["id"], r.get("created_at"), js
            break
    if not jobs:
        info["진행률"] = "시작한 잡이 있는 실행이 없다"
        return

    per, unread = {}, 0
    for j in jobs:
        rc, out, _ = sh("gh", "api", f"repos/{REPO}/actions/jobs/{j['id']}/logs")
        if rc != 0:
            unread += 1
            continue
        m = PG.search(out)
        if not m:
            unread += 1
            continue
        mem = int(m.group(1))
        todo = int(m.group(2).replace(",", ""))
        seen = int(m.group(3).replace(",", ""))
        d = per.setdefault(mem, {"받음": seen, "할일": []})
        d["받음"] = max(d["받음"], seen)
        d["할일"].append(todo)

    if not per:
        info["진행률"] = f"잡 {len(jobs)}개의 로그에서 진행 줄을 못 찾았다"
        return

    rows, tot_d, tot_a = {}, 0, 0
    for mem, d in sorted(per.items()):
        done, left = d["받음"], sum(d["할일"])
        allq = done + left
        rows[f"m{mem}"] = {"받음": done, "전체": allq, "남은": left,
                           "비율": round(done / allq * 100, 1) if allq else 0.0,
                           "읽은구간": len(d["할일"])}
        tot_d += done
        tot_a += allq
    info["진행률"] = {"실행": rid, "생성": created, "멤버": rows,
                      "합계받음": tot_d, "합계전체": tot_a,
                      "합계비율": round(tot_d / tot_a * 100, 1) if tot_a else 0.0,
                      "로그못읽은잡": unread}

    # 안 늘었나 — **다른 실행**과 비교할 때만
    prev, prev_t = _last_progress()
    prev_rid = None
    hp = os.path.join(OUT, "history.jsonl")
    if os.path.exists(hp):
        for line in io.open(hp, encoding="utf-8"):
            if line.strip():
                try:
                    prev_rid = (json.loads(line).get("정보") or {}).get(
                        "진행률", {}).get("실행")
                except Exception:
                    pass
    if PROGRESS_STALL and prev and prev_rid and prev_rid != rid:
        stuck = [k for k, v in rows.items()
                 if k in prev and prev[k] is not None and v["받음"] <= prev[k]]
        if stuck:
            bad.append(f"진행률이 안 늘었다 — {', '.join(sorted(stuck))} "
                       f"(지난 판정 {prev_t} · 실행 {prev_rid} → {rid})")


def check_runs(bad, info):
    if REPO:
        runs = gh_json(f"repos/{REPO}/actions/workflows/night.yml/runs?per_page=12")
        if runs:
            night = runs.get("workflow_runs", [])
            cc = collections.Counter(
                (r.get("conclusion") or r.get("status")) for r in night[:12])
            info["최근실행결론"] = dict(cc)
            if night and not any((r.get("conclusion") == "success"
                                  or r.get("status") in ("in_progress", "queued"))
                                 for r in night[:8]):
                bad.append(f"최근 `{WF_NAME}` 실행 8건에 성공도 진행중도 없다 — {dict(cc)}")

    rc, out, _ = sh("git", "log", "-400", "--format=%ct%x09%s")
    newest = None
    for line in out.splitlines():
        parts = line.split("\t", 1)
        if len(parts) == 2 and parts[1].startswith("night "):
            newest = int(parts[0])
            break
    if newest is None:
        bad.append("최근 400커밋에 `night ` 커밋이 없다 — 수집이 멈췄다")
    else:
        age = (time.time() - newest) / 60
        info["마지막수집커밋"] = f"{age:.0f}분 전"
        if age > COMMIT_STALE_MIN:
            if _is_done(info):
                info["마지막수집커밋_사유"] = (
                    f"완주 {info['진행률']['합계비율']}% — 오래된 것이 맞다. "
                    f"이상으로 잡지 않는다.")
            else:
                bad.append(f"마지막 `night ` 커밋이 **{age:.0f}분 전**이다 "
                           f"— 문턱 {COMMIT_STALE_MIN}분 초과")


# ── 상태 파일 ────────────────────────────────────────────────────────
def write_status(bad, info, members):
    os.makedirs(OUT, exist_ok=True)
    now = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    L = []
    L.append(f"# GEFS night 감시 판정 — {now}")
    L.append("")
    L.append(f"저장소: `{REPO or '(모름)'}`")
    L.append("")
    L.append(f"## 판정: {'★ 이상 ' + str(len(bad)) + '건' if bad else '정상'}")
    L.append("")
    if bad:
        for b in bad:
            L.append(f"- {b}")
    else:
        L.append("- 멤버별 실패율·속도, 잡 물결, 연도 파일 충돌, 실행 상태, 커밋 신선도, 진행률 모두 통과.")
    L.append("")
    L.append(f"## 멤버별 (최근 {LOOKBACK_H}시간 커밋 기준)")
    L.append("")
    if members:
        L.append("| 멤버 | 커밋 | 판정 실패율 (근거 ok/fail) | 최악(참고) | 중앙 "
                 "| 중앙 단위/시간 | 성공 0건 커밋 |")
        L.append("|---|---|---|---|---|---|---|")
        for m in sorted(members):
            r = members[m]
            L.append(f"| m{m} | {r['커밋수']} | "
                     f"{100*r['판정실패율']:.1f}% "
                     f"({r['판정근거_ok']:,}/{r['판정근거_fail']:,}) | "
                     f"{100*r['최악실패율_참고']:.1f}% | {100*r['중앙실패율']:.1f}% | "
                     f"{r['중앙속도']:.0f} | {r['성공0건커밋']} |")
        L.append("")
        L.append("⚠ **합계를 적지 않는다.** 커밋의 `ok`·`fail` 은 세션 누적값이고 한 멤버에 "
                 "잡이 여러 개 도는데 커밋 메시지에 구간이 없어 섞인다. 합쳤다가 "
                 "「m3 실패율 25.6%」라는 없는 이상을 만든 적이 있다 (2026-09-01). "
                 "그래서 **커밋 하나 안의 비율**만 본다.")
    else:
        L.append("(자료 없음)")
    L.append("")

    # ── 진행률 ──
    pg = info.get("진행률")
    L.append("## 진행률 (잡 로그의 `할 일 N (이미 N)` 기준)")
    L.append("")
    if isinstance(pg, dict) and pg.get("멤버"):
        L.append(f"실행 `{pg.get('실행')}` · {str(pg.get('생성'))[:16]} 기준")
        L.append("")
        L.append("| 멤버 | 받음 | 전체 | 남은 | 비율 |")
        L.append("|---|---|---|---|---|")
        for k in sorted(pg["멤버"], key=lambda x: int(x[1:])):
            r = pg["멤버"][k]
            L.append(f"| {k} | {r['받음']:,} | {r['전체']:,} | "
                     f"{r['남은']:,} | **{r['비율']}%** |")
        L.append(f"| **합계** | {pg['합계받음']:,} | {pg['합계전체']:,} | "
                 f"{pg['합계전체'] - pg['합계받음']:,} | **{pg['합계비율']}%** |")
        L.append("")
        if pg.get("로그못읽은잡"):
            L.append(f"⚠ 잡 {pg['로그못읽은잡']}개는 로그를 못 읽어 빠졌다 — "
                     f"그 구간의 `할 일` 이 안 들어갔으므로 **전체가 실제보다 작고 "
                     f"비율은 실제보다 높다.**")
            L.append("")
        L.append("⚠ `받음` 은 잡이 **시작한 시점**의 값이다. 실행 중이면 그 뒤로 더 "
                 "받았으므로 실제 진행률은 이보다 조금 높다. 그래서 「안 늘었다」 "
                 "판정은 **실행 id 가 바뀐 뒤**에만 한다 — 같은 실행을 두 번 읽으면 "
                 "당연히 같은 값이다.")
    else:
        L.append(f"({pg if pg else '자료 없음'})")
    L.append("")

    L.append("## 그 밖")
    L.append("")
    L.append("```json")
    L.append(json.dumps(info, ensure_ascii=False, indent=1))
    L.append("```")
    L.append("")
    L.append("---")
    L.append("")
    L.append("이 파일은 `watchdog_night.py` 가 3시간마다 덮어쓴다. 추세는 "
             "`_watchdog/history.jsonl`. 판정 근거와 문턱의 이유는 스크립트 머리말에 있다.")
    io.open(os.path.join(OUT, "status.md"), "w", encoding="utf-8").write("\n".join(L) + "\n")

    rec = {"t": now, "이상수": len(bad), "이상": bad, "멤버": members, "정보": info}
    with io.open(os.path.join(OUT, "history.jsonl"), "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def main():
    dry = "--dry" in sys.argv
    bad, info = [], {}
    # ★ 진행률을 **먼저** 잰다 — check_members·check_runs 의 「커밋이 없다/오래됐다」
    #   판정이 완주 여부를 봐야 하기 때문이다 (2026-09-03, 계정 A 완주 오탐).
    check_progress(bad, info)
    members = check_members(bad, info)
    check_wave(bad, info)
    check_spans(bad, info)
    check_runs(bad, info)

    print(f"저장소 {REPO or '(모름)'}")
    print("=" * 70)
    if members:
        for m in sorted(members):
            r = members[m]
            print(f"  m{m}: 커밋 {r['커밋수']:>4} (판정 {r['판정에쓴커밋']:>4}) · "
                  f"판정 실패율 {100*r['판정실패율']:>5.1f}% "
                  f"(ok {r['판정근거_ok']:,} fail {r['판정근거_fail']:,}) · "
                  f"최악(참고) {100*r['최악실패율_참고']:>5.1f}% · "
                  f"중앙 {100*r['중앙실패율']:>5.1f}% · "
                  f"{r['중앙속도']:>6.0f}단위/시간 · 성공0건 {r['성공0건커밋']}")
    print()
    for k, v in info.items():
        print(f"  {k}: {json.dumps(v, ensure_ascii=False)[:300]}")
    print()
    print("=" * 70)
    if bad:
        print(f"★ 이상 {len(bad)}건")
        for b in bad:
            print("   ·", b)
    else:
        print("정상")

    if not dry:
        write_status(bad, info, members)
        print(f"\n상태 파일: _watchdog/status.md · _watchdog/history.jsonl")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

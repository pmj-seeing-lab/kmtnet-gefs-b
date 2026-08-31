# -*- coding: utf-8 -*-
"""GEFS 야간 촘촘 수집기 — 밤 안 3시간 간격 + 변수 확장 (2026-08-28).

★ **`worker_v2.py` 를 건드리지 않는다.** 별도 산출 경로(`data/night`)에 쓴다.
   v2 는 24h 배수 리드로 2017~2026 을 이미 받아 놨고 그건 그대로 정본이다.

무엇이 달라졌나
  ① **밤 안 3시간 간격 4시각** — v2 는 밤당 00 UT 한 점이었다.
     실측(2026-08-28): CTIO 에서 1시각 0.469 → 4시각 평균 0.539, **+0.071 [+0.032, +0.110] 유의**.
     사이트마다 밤이 다른 UT 대에 있어 **리드 목록이 사이트마다 다르다**:
        CTIO  밤 0·3·6·9 UT   → 리드 24k+{0,3,6,9}
        SAAO  밤 18·21·0·3 UT → 리드 24k+{-6,-3,0,3}
        SSO   밤 9·12·15·18 UT→ 리드 24k+{9,12,15,18} (24(k-1) 기준)
  ② **변수 확장** — v2 는 기압면 t·u·v 뿐이었다. 여기에 더한다:
        기압면  HGT(지오포텐셜 고도) · RH
        지표    DLWRF·DSWRF(하향 장·단파) · SHTFL·LHTFL(현열·잠열) · GUST · HPBL · FRICV · VIS
        2m      SPFH · DPT
     ⚠ HGT 가 있어야 TopoSCALE 표고 내삽을 **그 시각 실제 높이**로 한다
       (지금은 고정표 근사). 하향 장파는 야간 지면 냉각 = 접지층 난류의 구동원인데 통째로 없었다.
     ⚠ 변수를 늘려도 **파일 수는 안 늘어난다** — 같은 파일 안 바이트만 1.8배(18.6→33.9 MB).
  ③ **병렬 다운로드** — 실측 5.1배 (147초/작업 → 28.5초).
     v2 는 순차였다. 여기서는 (사이클, 리드) 작업을 스레드 풀로 돌린다.

멤버 (사용자 지시 2026-08-28, 같은 날 두 번)
  · 처음: 멤버 5개 유지 → **뒤집힘: 멤버 31개 전부** (c00 + p01~p30).
    GitHub 러너 실측이 레인당 836~991작업/시간으로 예상보다 9~14배 빨라서 감당이 된다.
    5개로는 「GEFS 를 쓰면서 앙상블을 왜 안 썼나」에 답할 수 없다.
  · ⚠ 멤버 0 만 atmos.25(TCDC·PWAT)를 추가로 받는다 → 1~30 은 작업당 비용이 2/3.

유지하는 것
  · **격자 최근접점 1개** — 3×3 실험에서 양의 유의 0/42 였다
  · 4사이트 동시 추출 (파일 하나로 paranal·ctio·saao·sso)
  · **0.25° 격자는 못 쓴다** — GEFS atmos.25 는 기압면이 CAPE·CIN 2개뿐이다
    (2026-08-28 실측). 지표만 나중에 따로 받는다.

규모: 리드 56종 × 발행일 1,962 × 멤버 31 = **3,406,032 작업**.
기간: 2021-04-01 ~ 2026-08-14 (KMTNet 라벨 범위). 재개형이라 이미 받은 (사이클, 리드) 는 건너뛴다.
산출: data/night/{YYYY}_m{M}.jsonl.gz  — v2 의 data/v2 와 섞이지 않는다
"""
import glob
import gzip
import json
import os
import random
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

sys.stdout.reconfigure(encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data", "night")
os.makedirs(DATA, exist_ok=True)
BRANCH = os.environ.get("BRANCH", "master")

SITES = {  # lon 은 0~360 (worker_v2 와 동일)
    "paranal": (-24.6275, 289.5956),
    "ctio":    (-30.1690, 289.1946),
    "saao":    (-32.3790, 20.8107),
    "sso":     (-31.2720, 149.0620),
}
LEV_A = [10, 50, 100, 200, 250, 500, 700, 850, 925, 1000]
LEV_B = [1, 2, 3, 5, 7, 20, 30, 70, 150, 300, 350, 400, 450, 550, 600, 650, 750, 800, 900, 950, 975]
LEVELS = sorted(LEV_A + LEV_B)                       # 31층 (v2 와 같은 순서)

# 사이트별 밤 UT 시각 (3시간 배수 4개). era5_features.night_hours 실측에서 뽑음.
NIGHT_H = {"ctio": [0, 3, 6, 9], "saao": [18, 21, 0, 3], "sso": [9, 12, 15, 18]}
DAYS = list(range(1, 8))                             # 리드 1~7일


# ★★ GEFS 는 2020-09-24 사이클부터 **3시간 간격**이고, 그 전에는 **6시간 간격**이다.
#   (v12 운영 전환 2020-09-23. 자료에서 확인한 첫 3시간 간격 사이클이 2020-09-24 다.)
#
#   2026-09-01 에 이것 때문에 이 저장소의 잡이 죽어 있었다. `needed_leads()` 가 날짜와
#   무관하게 56개를 내놓아, 2018~2020-02 구간 잡들이 **존재하지 않는 리드(15·21·27···)**
#   를 5시간 동안 두드렸다. m5·m8 의 그 구간이 `ok 0 fail 5,617` / `ok 0 fail 6,195`
#   (각 301분)였고, 로그가 17KB 였다(정상 잡은 37MB — herbie GRIB 출력이 아예 없었다).
#   계정 A 에서 같은 증상의 커밋 메시지에 이유가 찍혔다 —
#       `ValueError: No index file was found for None` x3,444 · `KeyError: 'href'` x567
#
#   창고에서 센 근거: v11 시기 리드는 **28개, 전부 6의 배수**(12·18···174),
#   v12 시기는 **56개**(12·15···177). `gefs_night` 과 `gefs_v2` 가 독립적으로 같은
#   경계(2020-09-24)를 가리킨다.
V12_3H_FROM = "2020-09-24"


def needed_leads(cycle=None):
    """4사이트 합집합 리드. 파일 하나로 4곳을 다 뽑으므로 합집합만 받으면 된다.

    `cycle` 을 주면 **그 날에 실제로 존재하는 리드만** 돌려준다 —
    2020-09-24 이전은 6시간 간격뿐이므로 6의 배수만 남긴다."""
    L = set()
    for hs in NIGHT_H.values():
        for k in DAYS:
            for h in hs:
                lead = 24 * k + h if h < 12 else 24 * (k - 1) + h
                if 0 < lead <= 192:
                    L.add(lead)
    if cycle is not None and str(cycle)[:10] < V12_3H_FROM:
        L = {x for x in L if x % 6 == 0}
    return sorted(L)


# ── 검색 정규식 (v2 대비 확장분에 ★) ──────────────────────────
SEARCH_PL = r":(HGT|TMP|UGRD|VGRD|RH):(\d+) mb"       # ★ HGT·RH 추가
SEARCH_SFC = (r":(UGRD|VGRD):10 m above|:(TMP|RH|SPFH|DPT):2 m above|"
              r":(PRES|DLWRF|DSWRF|SHTFL|LHTFL|GUST|HPBL|FRICV|VIS):surface")  # ★ 복사·플럭스
SEARCH_EXT = r":(TCDC|PWAT):"
# ★ atmos.5b 전용 — atmos.5 에 없는 것들. 파일 수는 안 늘고 바이트만 는다.
#   해발 고정 고도면(914·1829·2743 m)은 사이트 표고(SSO 1037·SAAO 1761·CTIO 2125 m) 바로 옆이라
#   **TopoSCALE 내삽 없이 쓸 수 있는 대조군**이 된다.
SEARCH_5B = (r":(GUST|HPBL|FRICV|VIS|SUNSD):surface|:(SPFH|DPT):2 m above|"
             r":VRATE:planetary boundary layer|"
             r":(TMP|UGRD|VGRD):(914|1829|2743) m above mean sea level")
PL_VARS = ("gh", "t", "u", "v", "r")


def sh(*a):
    return subprocess.run(a, cwd=HERE, capture_output=True, text=True)


def commit_push(msg):
    # ⚠ **git add -A 를 쓰면 안 된다** (2026-08-28 실측).
    #    data/v2 의 원본 gz 1,740 개가 창고 적재 뒤 prune 되어 작업트리에서 이미 지워져 있다.
    #    add -A 면 그 삭제가 통째로 커밋된다. **우리가 만든 것만 담는다.**
    sh("git", "add", "data/night")
    if sh("git", "diff", "--cached", "--quiet").returncode == 0:
        return True
    sh("git", "commit", "-m", msg)
    for i in range(10):
        sh("git", "pull", "--rebase", "--autostash", "origin", BRANCH)
        if sh("git", "push", "origin", f"HEAD:{BRANCH}").returncode == 0:
            return True
        sh("git", "rebase", "--abort")
        time.sleep(random.uniform(2, 8) * (1 + i * 0.5))
    print("push 실패 10회 — 다음 커밋 때 재시도", flush=True)
    return False


def purge_cache():
    import shutil
    for d in (os.path.expanduser("~/data"), os.path.expanduser("~/.cache/herbie")):
        shutil.rmtree(d, ignore_errors=True)


def rd(x):
    return None if x is None or not np.isfinite(x) else round(float(x), 2)


def extract_points(H, search, want):
    """search 로 받아 4사이트 최근접점 추출 → {var: {lev: {site: val}}}. (v2 와 같은 계약)"""
    out = {}
    dss = H.xarray(search, remove_grib=True)
    if not isinstance(dss, list):
        dss = [dss]
    for ds in dss:
        pts = {s: ds.sel(latitude=la, longitude=lo, method="nearest")
               for s, (la, lo) in SITES.items()}
        for var in ds.data_vars:
            if want and var not in want:
                continue
            ref = pts["paranal"]
            if "isobaricInhPa" in ref[var].coords:
                lvs = np.atleast_1d(ref["isobaricInhPa"].values).astype(int)
                for s, p in pts.items():
                    vv = np.atleast_1d(p[var].values).ravel()
                    for i, L in enumerate(lvs):
                        out.setdefault(var, {}).setdefault(int(L), {})[s] = float(vv[i])
            elif any(c in ref[var].coords for c in ("heightAboveSea", "heightAboveGround")):
                # ★ 해발/지상 고정 고도면 — 고도를 키에 넣지 않으면 914·1829·2743 m 가 서로 덮어쓴다
                cn = "heightAboveSea" if "heightAboveSea" in ref[var].coords else "heightAboveGround"
                hh = np.atleast_1d(ref[cn].values).astype(int)
                for s, p in pts.items():
                    vv = np.atleast_1d(p[var].values).ravel()
                    for i, H_ in enumerate(hh):
                        out.setdefault(var, {}).setdefault(f"h{int(H_)}", {})[s] = float(vv[i])
            else:
                for s, p in pts.items():
                    out.setdefault(var, {}).setdefault(None, {})[s] = \
                        float(np.atleast_1d(p[var].values).ravel()[0])
    return out


def _iter(d):
    for var, levs in d.items():
        for lev, sv in levs.items():
            yield var, lev, sv


FAILWHY = {}   # 실패 이유별 횟수 — 커밋 메시지에 실어 보낸다
#  ★ 2026-09-01: 계정 A 에만 있던 계측을 이 저장소로 옮겼다.
#    A 는 커밋 메시지에 `| throttle(503/429) x6` 이 찍혀 왜 느린지 보이는데, B 는
#    `ok / fail` 만 찍혀서 **느린 이유를 볼 방법이 없었다.** B 의 잡당 속도가
#    A 의 2/3(중앙 590/h vs 900/h)인 것을 확인했는데 원인을 좁힐 자료가 없다.


def fetch_unit(cycle, fxx, member, retry=2):
    from herbie import Herbie
    for k in range(retry + 1):
        try:
            acc = {}
            Ha = Herbie(cycle, model="gefs", member=member, fxx=fxx,
                        product="atmos.5", verbose=False)
            for v, lev, sv in _iter(extract_points(Ha, SEARCH_PL, PL_VARS)):
                acc.setdefault(v, {})[lev] = sv
            sfc = extract_points(Ha, SEARCH_SFC, None)
            Hb = Herbie(cycle, model="gefs", member=member, fxx=fxx,
                        product="atmos.5b", verbose=False)
            for v, lev, sv in _iter(extract_points(Hb, SEARCH_PL, PL_VARS)):
                acc.setdefault(v, {})[lev] = sv
            try:                                   # ★ atmos.5b 지표·고도면 (없어도 진행)
                for v, lev, sv in _iter(extract_points(Hb, SEARCH_5B, None)):
                    sfc.setdefault(v, {})[lev] = sv
            except Exception:
                pass
            ext = None
            if member == 0:
                try:
                    H25 = Herbie(cycle, model="gefs", member=0, fxx=fxx,
                                 product="atmos.25", verbose=False)
                    ext = extract_points(H25, SEARCH_EXT, None)
                except Exception:
                    ext = None
            return acc, sfc, ext
        except Exception as e:
            s = str(e)
            if "Slow Down" in s or "503" in s or "429" in s:
                FAILWHY["throttle(503/429)"] = FAILWHY.get("throttle(503/429)", 0) + 1
                time.sleep(10 * (k + 1)); continue
            if k >= retry:
                # ⚠ **이유를 남긴다.** 그냥 None 을 돌려주면 무엇이 문제인지 못 좁힌다.
                key = f"{type(e).__name__}: {s[:60]}"
                FAILWHY[key] = FAILWHY.get(key, 0) + 1
                return None
            time.sleep(4)
    FAILWHY["throttle 재시도 소진"] = FAILWHY.get("throttle 재시도 소진", 0) + 1
    return None


def rows_from(cycle, valid, member, fxx, acc, sfc, ext):
    rows = []
    for s in SITES:
        r = {"site": s, "cycle": cycle, "valid": valid, "member": member,
             "lead": fxx, "lv": LEVELS}
        for var in PL_VARS:
            r[var] = [rd(acc.get(var, {}).get(L, {}).get(s)) for L in LEVELS]
        for var, levs in sfc.items():
            for lev, sv in levs.items():   # lev 는 None 또는 "h914" 같은 고도 키
                nm = f"sfc_{var}" if lev is None else f"sfc_{var}_{lev}"
                r[nm.lower()] = rd(sv.get(s))
        u10, v10 = r.get("sfc_u10"), r.get("sfc_v10")
        if u10 is not None and v10 is not None:
            r["sfc_ws10"] = rd(float(np.hypot(u10, v10)))
        if ext:
            for var, levs in ext.items():
                for lev, sv in levs.items():
                    r[{"tcc": "tcdc"}.get(var.lower(), var.lower())] = rd(sv.get(s))
        rows.append(r)
    return rows


def done_keys(member):
    """이미 받은 (cycle, lead) — 재개형."""
    seen = set()
    for fn in glob.glob(os.path.join(DATA, f"*_m{member}.jsonl.gz")):
        try:
            with gzip.open(fn, "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        r = json.loads(line)
                    except Exception:
                        continue
                    if r.get("site") == "paranal":
                        seen.add((r["cycle"], r["lead"]))
        except Exception:
            pass
    return seen


def main():
    import pandas as pd
    M = int(os.environ.get("MEMBER", "0"))
    WORKERS = int(os.environ.get("WORKERS", "6"))
    START = os.environ.get("START", "2021-04-01")
    END = os.environ.get("END", "2026-08-14")
    BUDGET = float(os.environ.get("BUDGET_MIN", "300")) * 60      # 세션 예산(초)
    COMMIT_EVERY = int(os.environ.get("COMMIT_EVERY", "40"))

    # STRIDE · 발행일을 몇 일마다 받을지. 1 이면 매일.
    #   멤버 5~30 은 「앙상블 멤버를 늘리는 것이 값어치가 있나」를 재려고 받는 것이지
    #   그 자체가 최종 자료가 아니다. 날짜를 솎으면 그 질문에 훨씬 빨리 답할 수 있고,
    #   답이 「값어치 있다」면 STRIDE 를 1 로 되돌려 그대로 이어받으면 된다(재개형).
    #   솎아도 밤 대부분은 덮인다 — 발행일 하나가 앞으로 7일 밤을 덮기 때문이다.
    STRIDE = max(1, int(os.environ.get("STRIDE", "1")))
    cyc = pd.date_range(START, END, freq="D")
    if STRIDE > 1:
        # 기준을 START 가 아니라 고정 원점으로 잡는다 — 기간 분할이 달라도 같은 날을 고른다
        cyc = cyc[((cyc - pd.Timestamp("2021-04-01")).days % STRIDE) == 0]
    cycles = [c.strftime("%Y-%m-%d %H:%M") for c in cyc]
    seen = done_keys(M)
    # ★ 리드를 **사이클마다** 고른다 — 2020-09-24 이전은 6시간 간격뿐이다.
    #   이 줄이 예전에는 `for f in leads`(고정 56개)였고, 그래서 옛 구간 잡이
    #   없는 파일을 두드리며 5시간을 태웠다 (m5·m8 성공 0건).
    todo = [(c, f) for c in cycles for f in needed_leads(c) if (c, f) not in seen]
    n_lead_lo = len(needed_leads("2018-01-01"))
    n_lead_hi = len(needed_leads("2024-01-01"))
    print(f"[m{M}] 리드 {n_lead_lo}개(2020-09-24 이전) / {n_lead_hi}개(이후) · "
          f"사이클 {len(cycles):,} · "
          f"할 일 {len(todo):,} (이미 {len(seen):,}) · 워커 {WORKERS}", flush=True)
    if not todo:
        print("할 일 없음"); return

    t0 = time.time(); ok = fail = 0
    buf = {}
    with ThreadPoolExecutor(WORKERS) as ex:
        futs = {}
        it = iter(todo)
        def submit_more(n):
            for _ in range(n):
                try:
                    c, f = next(it)
                except StopIteration:
                    return
                futs[ex.submit(fetch_unit, c, f, M)] = (c, f)
        submit_more(WORKERS * 3)
        while futs:
            if time.time() - t0 > BUDGET:
                print("세션 예산 소진 — 정리하고 종료", flush=True)
                break
            fu = next(as_completed(list(futs)))
            c, f = futs.pop(fu)
            try:
                got = fu.result()
            except Exception:
                got = None
            if got is None:
                fail += 1
            else:
                valid = (pd.Timestamp(c) + pd.Timedelta(hours=f)).strftime("%Y-%m-%d %H:%M")
                yr = c[:4]
                buf.setdefault(yr, []).extend(rows_from(c, valid, M, f, *got))
                ok += 1
            submit_more(1)
            if (ok + fail) % COMMIT_EVERY == 0 and buf:
                for yr, rows in buf.items():
                    p = os.path.join(DATA, f"{yr}_m{M}.jsonl.gz")
                    with gzip.open(p, "at", encoding="utf-8") as g:
                        for r in rows:
                            g.write(json.dumps(r, ensure_ascii=False) + "\n")
                buf.clear()
                el = time.time() - t0
                rate = ok / el * 3600 if el else 0
                why = ""
                if FAILWHY:
                    top = sorted(FAILWHY.items(), key=lambda kv: -kv[1])[:2]
                    why = " | " + " · ".join(f"{k} x{v}" for k, v in top)
                commit_push(f"night m{M} ok {ok} fail {fail} ({rate:.0f}/h){why}")
                print(f"  ok {ok:,} fail {fail} · {el/60:.1f}분 · {rate:.0f}작업/시간", flush=True)
                purge_cache()
    for yr, rows in buf.items():
        p = os.path.join(DATA, f"{yr}_m{M}.jsonl.gz")
        with gzip.open(p, "at", encoding="utf-8") as g:
            for r in rows:
                g.write(json.dumps(r, ensure_ascii=False) + "\n")
    el = time.time() - t0
    commit_push(f"night m{M} 세션종료 ok {ok} fail {fail} ({ok/el*3600 if el else 0:.0f}/h)")
    print(f"[m{M}] 종료 · ok {ok:,} fail {fail} · {el/60:.1f}분 · "
          f"{ok/el*3600 if el else 0:.0f}작업/시간", flush=True)


if __name__ == "__main__":
    if "--plan" in sys.argv:
        import pandas as pd
        L = needed_leads()
        cyc = len(pd.date_range("2021-04-01", "2026-08-14", freq="D"))
        print(f"리드 {len(L)}개: {L}")
        print(f"사이클 {cyc:,} · 멤버 5 → 작업 {len(L)*cyc*5:,}")
    else:
        main()

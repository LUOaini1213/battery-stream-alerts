# PIT mutation testing (bsa-core alert rules)

Command: `scripts/mvn-docker.sh -B -pl bsa-core test-compile org.pitest:pitest-maven:mutationCoverage`
(PIT 1.17.0, `STRONGER` mutator group, target classes `bsa.core.rules.*`). Run 2026-10-13.

| run | mutants | killed | survived | no coverage | mutation score | line coverage of mutated classes |
|---|---|---|---|---|---|---|
| 1 - original test suite | 233 | 204 | 29 | 0 | 87.6% | 261/267 (98%) |
| 2 - after adding `MutationGapTest` | 233 | 219 | 14 | 0 | **94.0%** | see PIT HTML report |

Run 1's survivors pointed at real gaps: the initial-capacity guard was only exercised with zero
cycles, the drop-alert text was never checked, the sag threshold was never tested at exact equality,
`WindowStats.add` was only used for its side effect, sorting a cycle by sequence number (rather than
by clock) was untested, and the boundaries in `Thresholds.validate()` were not pinned.
`MutationGapTest` adds one test per gap.

## The 14 remaining survivors, and why they are left

11 are equivalent mutants (no input can tell them apart from the original); 3 are a deliberate
gap in a field nobody consumes:

| class:line | mutant | why it is equivalent |
|---|---|---|
| FadeTracker:56 | `isNaN(initial)` -> false | `capacity / NaN` is NaN anyway |
| FadeTracker:58 | `!isNaN(rolling)` -> true | `x < NaN` is false anyway |
| FadeTracker:66 | `!isNaN(soh)` -> true | `NaN < 0.8` is false anyway |
| FadeTracker:50 | short-cycle `return out` -> empty list | `out` is empty at that point |
| WindowRules:19, 20 | `count == 0` early return removed / returns empty list | with zero samples neither rule can fire; the list is empty either way |
| WindowStats:44 | `t - anchor` -> `t + anchor` | shifts every x by the same constant; a least-squares slope is shift-invariant |
| WindowStats:73 | `loadCount < 2` guard removed | with 0 or 1 point the denominator is exactly 0, which the next guard turns into NaN |
| WindowStats:75 (x2) | `abs(denom) < 1e-12` boundary / removed | denominator is either exactly 0 (-> 0/0 = NaN) or far above 1e-12 for real timestamps |
| CycleAnalyzer:97 (x3) | `max(0, dt)` in a gap alert's window start | not strictly equivalent: differs only when a sequence hole coincides with a backwards clock step, and then only in the informational `window_start_ms` of the gap alert. Left untested (low value) |
| CycleAnalyzer:38 | secondary sort key (time) returns 0 | samples with equal seq are skipped as duplicates, so their relative order is irrelevant |

Reports: `bsa-core/target/pit-reports/index.html` (not committed; regenerate with `make pit`).

.PHONY: venv train scan baseline train_many train_many_all crash_unsafe \
        trace_one timeline plot_timeline rollback_latest \
        train_torch scan_torch baseline_torch \
		train_many_torch crash_unsafe_torch summary_torch\
		clean clean_all clean_group \ 
		group_fuzz group_fuzz_summary summary_group bench_group summary_bench\
		params \
		group_scan_one group_rollback

# ---- shared defaults (edit here once and everything follows) ----
EPOCHS            ?= 120
EVERY             ?= 3
PAUSE_MS          ?= 1000
SEED              ?= 0
IOSTAT_INTERVAL   ?= 1
SEEDS             ?= 0-9
MODES             ?= none,bitflip,truncate,zerorange
CRASH_MODES       ?= early,mid,late
KB_MODEL          ?= 128
KB_OPTIM          ?= 64

# ---- environment ----
venv:
	python3 -m venv .venv && . .venv/bin/activate && python -m pip install --upgrade pip wheel numpy pandas matplotlib tqdm

# ---- NumPy pipeline (single run) ----
train:
	. .venv/bin/activate && python -m src.aiwork.ckpt_writer \
	  --epochs $(EPOCHS) --checkpoint-every $(EVERY) --out trace/ckpts \
	  --seed $(SEED) --fault none --write-mode atomic --pause-ms $(PAUSE_MS)

scan:
	. .venv/bin/activate && python -m src.guard.integrity_guard \
	  --ckpt-dir trace/ckpts --out trace/guard/ckpt_scan.csv

baseline: train scan

# ---- NumPy pipeline (many runs / FI matrix) ----
train_many:
	. .venv/bin/activate && python tools/run_many.py \
	  --seeds $(SEEDS) --epochs $(EPOCHS) --every $(EVERY) \
	  --modes $(MODES) --write-mode atomic

# alias (kept for compatibility)
train_many_all: train_many

# Crash-consistency (unsafe writes + crash timing)
crash_unsafe:
	. .venv/bin/activate && python tools/run_many.py \
	  --seeds $(SEEDS) --epochs $(EPOCHS) --every $(EVERY) \
	  --modes none --write-mode unsafe --crash $(CRASH_MODES)

# ---- Tracing wrapper (NumPy) ----
trace_one:
	. .venv/bin/activate && python -m tools.run_with_trace \
	  --epochs $(EPOCHS) --every $(EVERY) --seed $(SEED) --fault none --write-mode atomic \
	  --out trace/ckpts_traced/run_0 --pause-ms $(PAUSE_MS) --iostat-interval $(IOSTAT_INTERVAL)

timeline:
	. .venv/bin/activate && python tools/xlayer_timeline.py \
		--app-log trace/sys/run_*applog --fs-usage trace/sys/run_*fs_usage --iostat trace/sys/run_*iostat \
		--out trace/timeline/timeline.csv

plot_timeline:
	. .venv/bin/activate && python -m tools.plot_timeline \
	  --timeline trace/timeline/timeline.csv --out figures/timeline.png

rollback_latest:
	. .venv/bin/activate && python -m src.guard.rollback \
	  --scan-csv trace/guard/ckpt_scan.csv --ckpt-root trace/ckpts --out-link trace/ckpts/latest_ok.npz

# ---- PyTorch pipeline (single run) ----
train_torch:
	. .venv/bin/activate && python -m src.aiwork.torch_ckpt_writer \
	  --epochs $(EPOCHS) --checkpoint-every $(EVERY) --out trace/ckpts_torch \
	  --seed $(SEED) --fault none --write-mode atomic --pause-ms $(PAUSE_MS)

scan_torch:
	. .venv/bin/activate && python -m src.guard.integrity_guard_pt \
	  --ckpt-dir trace/ckpts_torch --out trace/guard/ckpt_scan_torch.csv

baseline_torch: train_torch scan_torch

train_many_torch:
	. .venv/bin/activate && python tools/run_many_torch.py \
	  --seeds $(SEEDS) --epochs $(EPOCHS) --every $(EVERY) \
	  --modes $(MODES) --write-mode atomic

crash_unsafe_torch:
	. .venv/bin/activate && python tools/run_many_torch.py \
	  --seeds $(SEEDS) --epochs $(EPOCHS) --every $(EVERY) \
	  --modes none --write-mode unsafe --crash $(CRASH_MODES)

clean:
	rm -f trace/ckpts/latest_ok.npz
	rm -rf trace/ckpts trace/ckpts_torch trace/ckpts_runs trace/ckpts_runs_torch trace/ckpts_traced
	rm -rf trace/guard trace/sys trace/timeline figures

clean_all: clean
	rm -rf .venv

summary_torch:
	. .venv/bin/activate && python -m tools.summarize_torch --runs-root trace/guard/runs_torch \
	  --out-csv figures/torch_mode_summary.csv --out-md figures/torch_mode_summary.md --out-png figures/torch_mode_bars.png

group_fuzz:
	. .venv/bin/activate && python -m tools.run_group_fuzz \
	  --epochs $(EPOCHS) --every $(EVERY) --seeds $(SEEDS) \
	  --pause-ms $(PAUSE_MS) --kb-model $(KB_MODEL) --kb-optim $(KB_OPTIM)

group_fuzz_summary:
	@awk -F, 'NR==1{for(i=1;i<=NF;i++){if($$i=="group_ok")G=i;if($$i=="write_mode")W=i;if($$i=="crash_at")C=i}} \
	          NR>1{key=$$W"|"$$C; T[key]++; OK[key]+=($$G==1)} \
	          END{for(k in T){printf "%-22s : %d/%d (%.3f)\n", k, OK[k], T[k], (T[k]+0?OK[k]/T[k]:0)}}' \
	  trace/guard/group_scan_all.csv | sort

summary_group:
	. .venv/bin/activate && python -m tools.summarize_group \
	  --in trace/guard/group_scan_all.csv \
	  --out-csv figures/group_summary.csv \
	  --out-md figures/group_summary.md \
	  --out-png figures/group_bars.png \
	  --out-reasons figures/group_reasons.png

bench_group:
	. .venv/bin/activate && python -m tools.bench_group_ckpt \
	  --epochs $(EPOCHS) --every $(EVERY) --seeds $(SEEDS) \
	  --kb-model $(KB_MODEL) --kb-optim $(KB_OPTIM)

clean_group:
	rm -rf trace/groups trace/guard/group_scans trace/guard/group_scan*.csv \
	       figures/group_*.png figures/group_*.csv figures/group_*.md

params:
	@echo "EPOCHS=$(EPOCHS)  EVERY=$(EVERY)  SEEDS=$(SEEDS)  PAUSE_MS=$(PAUSE_MS)  KB_MODEL=$(KB_MODEL)KB  KB_OPTIM=$(KB_OPTIM)KB"

summary_bench:
	. .venv/bin/activate && python -m tools.summarize_bench \
	  --in-csv figures/bench_group.csv \
	  --out-summary figures/bench_summary.csv \
	  --out-overhead figures/bench_overhead.csv \
	  --out-md figures/bench_summary.md \
	  --out-png figures/bench_bars.png

GROUP_SCAN_ROOT ?= trace/groups/fuzz/atomic_seed0

group_scan_one:
	. .venv/bin/activate && python -m src.guard.group_guard \
	  --root $(GROUP_SCAN_ROOT) \
	  --out trace/guard/group_scan.csv

group_rollback:
	. .venv/bin/activate && python -m src.guard.group_rollback \
	  --scan trace/guard/group_scan.csv \
	  --out-link trace/groups/latest_ok

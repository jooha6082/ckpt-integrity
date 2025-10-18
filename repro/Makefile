.PHONY: repro_env repro_runs repro_figs repro_all
ROOT=..

repro_env:
	python3 -m venv $(ROOT)/.venv && . $(ROOT)/.venv/bin/activate && \
	  python -m pip install --upgrade pip wheel && \
	  python -m pip install numpy pandas matplotlib tqdm torch

repro_runs:
	. $(ROOT)/.venv/bin/activate && $(MAKE) -C $(ROOT) baseline_torch
	. $(ROOT)/.venv/bin/activate && $(MAKE) -C $(ROOT) train_many_torch
	. $(ROOT)/.venv/bin/activate && $(MAKE) -C $(ROOT) crash_unsafe_torch
	. $(ROOT)/.venv/bin/activate && $(MAKE) -C $(ROOT) group_fuzz
	. $(ROOT)/.venv/bin/activate && $(MAKE) -C $(ROOT) bench_group

repro_figs:
	. $(ROOT)/.venv/bin/activate && $(MAKE) -C $(ROOT) summary_torch
	. $(ROOT)/.venv/bin/activate && $(MAKE) -C $(ROOT) summary_group
	. $(ROOT)/.venv/bin/activate && $(MAKE) -C $(ROOT) summary_bench

repro_all: repro_env repro_runs repro_figs
	@echo "[repro] Done. See figures/ and trace/ under $(ROOT)/"

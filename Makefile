.PHONY: venv install train demo guard trace-mac trace-linux blktrace clean

venv:
	python3 -m venv .venv

install:
	. .venv/bin/activate && pip install --upgrade pip && pip install -e .

train:
	. .venv/bin/activate && ckpt-integrity-train --epochs 2 --ckpt-dir ckpt --device cpu

demo:
	. .venv/bin/activate && bash scripts/demo_fault.sh

guard:
	. .venv/bin/activate && ckpt-integrity-guard --watch ckpt --interval 1.0

trace-mac:
	bash scripts/trace_iostat_mac.sh

trace-linux:
	bash scripts/trace_iostat_linux.sh

blktrace:
	bash scripts/trace_blktrace_linux.sh

clean:
	rm -rf ckpt/*.pt ckpt/*.json trace/*.log trace/*.csv *.png

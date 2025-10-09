CKPT_DIR := ckpt
TRACE_DIR := trace

.PHONY: train watch verify1 inject_flip summarize io_join clean

train:
	# quick CPU run (fake data to avoid download)
	ckpt-integrity-train --epochs 3 --ckpt-dir $(CKPT_DIR) --device cpu --fake-data

watch:
	# guard directory watcher (1s poll)
	ckpt-integrity-guard --watch $(CKPT_DIR) --last-good $(CKPT_DIR)/last-good.pt

verify1:
	# sanity check
	ckpt-integrity-guard --verify $(CKPT_DIR)/epoch_1.pt --last-good $(CKPT_DIR)/last-good.pt

inject_flip:
	# flip a few bytes after epoch_1 is fully written
	f=$(CKPT_DIR)/epoch_1.pt; \
	while [ ! -f "$$f" ]; do sleep 0.2; done; \
	s1=0; s2=1; while [ "$$s1" != "$$s2" ]; do s1=$$(stat -f%z "$$f" 2>/dev/null || stat -c%s "$$f"); sleep 0.2; s2=$$(stat -f%z "$$f" 2>/dev/null || stat -c%s "$$f"); done; \
	ckpt-integrity-inject-flip "$$f" --nbytes 128

summarize:
	# ckpt/events.csv -> trace/events_summary.txt
	python tools/events_summary.py --events $(CKPT_DIR)/events.csv --out $(TRACE_DIR)/events_summary.txt
	@echo "wrote $(TRACE_DIR)/events_summary.txt"

io_join:
	# events(ok=False) ±5s join with iostat -> trace/event_io.csv
	python tools/event_io_join.py --events $(CKPT_DIR)/events.csv --iostat $(TRACE_DIR)/iostat.csv --window 5 --out $(TRACE_DIR)/event_io.csv
	@head -5 $(TRACE_DIR)/event_io.csv || true

clean:
	rm -f $(CKPT_DIR)/epoch_*.pt $(CKPT_DIR)/epoch_*.meta.json $(CKPT_DIR)/last-good.pt

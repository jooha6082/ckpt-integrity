# ckpt-integrity (mini-research, Step 1)

This repo seeds a minimal pipeline to:
1) create toy checkpoints (NPZ),
2) scan integrity (hash / size / loadability / NaN/Inf),
3) emit a CSV report under `trace/guard/ckpt_scan.csv`.

Next steps will add: filesystem tracing (fs_usage/iostat),
fault injection matrix, multi-layer guards, and recovery.

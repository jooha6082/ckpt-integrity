# Crash-Consistent AI Checkpoints (macOS/APFS)

**Mini‑research project for AI Infrastructure Reliability — with emphasis on Storage & Filesystem integrity.**  
This work explores *how AI training checkpoints can remain crash‑consistent, detectable for corruption, and recoverable automatically*.  
It provides a reproducible, small‑scale experiment that mirrors large‑scale reliability problems in data‑intensive AI systems.

---

## 🔧 Quick Start
```bash
git clone <git@github.com:jooha6082/ckpt-integrity.git> ckpt-integrity
cd ckpt-integrity
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
make -C repro repro_all        # one-click full experiment
```

---

## 🧩 Structure
| Folder | Role |
|---------|------|
| `src/aiwork` | checkpoint writers (single/group) |
| `src/guard`  | integrity scanner + rollback |
| `tools/` | summarizers, plotting, automation |
| `trace/` | outputs (ckpts, CSVs, logs) |
| `figures/` | generated charts |

---

## 🚀 Key Targets
```bash
make baseline_torch summary_torch      # single-file integrity
make group_fuzz summary_group          # group atomicity under crash
make bench_group summary_bench         # latency p50/p90/p99
make trace_one timeline plot_timeline  # cross-layer timeline
make rollback_latest                   # recovery demo
```

---

## 📊 Artifacts
| CSV | Figure |
|------|--------|
| `bench_summary.csv` | `bench_bars.png` |
| `bench_group.csv` | `bench_group_cdf.png` |
| `group_summary.csv` | `group_bars.png`, `groups_reasons.png` |
| `torch_mode_summary.csv` | `torch_mode_bars.png` |
| `timeline.csv` | `timeline.png` |

---

## 🧠 Summary
- **Problem:** AI training checkpoints can be *torn* by crashes or *silently corrupted* by storage faults.  
- **Method:** compare unsafe vs. atomic write protocols; add SHA‑256 integrity guard and rollback.  
- **Evaluation:** measure latency, robustness, and detection coverage under injected faults.  
- **Result:** atomic_dirsync fully prevents corruption, adding ~40–70 % latency overhead versus unsafe writes.

---

## 🧰 Reproduce
All results regenerate via:
```bash
make repro_all
```
Outputs → `trace/` (CSVs) and `figures/` (plots).

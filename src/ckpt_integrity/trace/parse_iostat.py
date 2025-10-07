import argparse, re, pandas as pd
PAT = re.compile(r'^(?P<device>\S+)\s+(?P<tps>[0-9.]+)\s+(?P<kB_read_s>[0-9.]+)\s+(?P<kB_wrtn_s>[0-9.]+)\s+(?P<kB_read>[0-9.]+)\s+(?P<kB_wrtn>[0-9.]+)')
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('log'); ap.add_argument('--out',default='trace/iostat.csv'); args=ap.parse_args()
    rows=[]; 
    with open(args.log) as f:
        for line in f:
            m=PAT.match(line.strip())
            if m:
                row=m.groupdict()
                for k in list(row.keys()):
                    if k!='device': row[k]=float(row[k])
                rows.append(row)
    pd.DataFrame(rows).to_csv(args.out, index=False)
    print(f"[OK] wrote {args.out} rows={len(rows)}")
if __name__=='__main__': main()

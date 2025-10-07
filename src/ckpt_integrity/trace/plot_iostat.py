import argparse, pandas as pd, matplotlib.pyplot as plt
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('csv'); ap.add_argument('--out',default='iostat_plot.png'); ap.add_argument('--y',default='kB_wrtn_s'); args=ap.parse_args()
    df=pd.read_csv(args.csv); 
    if args.y not in df.columns: raise SystemExit(f"{args.y} not in CSV")
    plt.figure(); plt.plot(df[args.y].values); plt.xlabel('sample idx (1s)'); plt.ylabel(args.y); plt.title('iostat write throughput'); plt.tight_layout(); plt.savefig(args.out,dpi=160)
    print(f"[OK] wrote {args.out}")
if __name__=='__main__': main()

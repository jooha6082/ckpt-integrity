import argparse, os, logging
from tqdm import tqdm
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
import torchvision.transforms as T
from ckpt_integrity.utils import write_meta

LOG = logging.getLogger("train")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

class FakeCIFAR(torch.utils.data.Dataset):
    def __init__(self, n=4096): self.n = n
    def __len__(self): return self.n
    def __getitem__(self, i):
        x = torch.rand(3, 32, 32)
        y = torch.randint(0, 10, (1,)).item()
        return x, y

def loaders(batch_size=128, workers=2, fake=False):
    if fake:
        train, test = FakeCIFAR(4096), FakeCIFAR(512)
    else:
        mean = (0.4914, 0.4822, 0.4465); std = (0.2470, 0.2435, 0.2616)
        t_train = T.Compose([T.RandomCrop(32, padding=4), T.RandomHorizontalFlip(), T.ToTensor(), T.Normalize(mean, std)])
        t_test  = T.Compose([T.ToTensor(), T.Normalize(mean, std)])
        train = torchvision.datasets.CIFAR10("data", train=True, download=True, transform=t_train)
        test  = torchvision.datasets.CIFAR10("data", train=False, download=True, transform=t_test)
    train_loader = torch.utils.data.DataLoader(train, batch_size=batch_size, shuffle=True, num_workers=workers)
    test_loader  = torch.utils.data.DataLoader(test,  batch_size=256, shuffle=False, num_workers=workers)
    return train_loader, test_loader

@torch.no_grad()
def eval_top1(model, loader, device):
    model.eval()
    ok, total = 0, 0
    for x, y in loader:
        x, y = x.to(device), y.to(device)
        pred = model(x).argmax(1)
        ok += (pred == y).sum().item()
        total += y.numel()
    return 100.0 * ok / total

def save_ckpt(state, ckpt_dir, step):
    os.makedirs(ckpt_dir, exist_ok=True)
    path = os.path.join(ckpt_dir, f"epoch_{step}.pt")
    torch.save(state, path)
    write_meta(path, step)
    last_good = os.path.join(ckpt_dir, "last-good.pt")
    torch.save(state, last_good)
    write_meta(last_good, step, extra={"alias": "last-good"})
    return path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=2)
    ap.add_argument("--batch-size", type=int, default=128)
    ap.add_argument("--lr", type=float, default=0.1)
    ap.add_argument("--ckpt-dir", type=str, default="ckpt")
    ap.add_argument("--device", type=str, default="cpu")  # cpu|cuda|mps
    ap.add_argument("--resume", type=str, default=None)
    ap.add_argument("--fake-data", action="store_true")
    args = ap.parse_args()

    dev = torch.device(args.device if (args.device != "cuda" or torch.cuda.is_available()) else "cpu")
    tr, te = loaders(args.batch_size, fake=args.fake_data)
    model = torchvision.models.resnet18(num_classes=10).to(dev)
    opt = optim.SGD(model.parameters(), lr=args.lr, momentum=0.9, weight_decay=5e-4)
    sch = optim.lr_scheduler.CosineAnnealingLR(opt, T_max=args.epochs)

    if args.resume and os.path.exists(args.resume):
        LOG.info("resume: %s", args.resume)
        state = torch.load(args.resume, map_location=dev)
        model.load_state_dict(state["model"]); opt.load_state_dict(state["opt"])

    for ep in range(1, args.epochs + 1):
        model.train()
        for x, y in tqdm(tr, desc=f"epoch {ep}/{args.epochs}"):
            x, y = x.to(dev), y.to(dev)
            opt.zero_grad()
            loss = nn.functional.cross_entropy(model(x), y)
            loss.backward()
            opt.step()
        sch.step()
        acc = eval_top1(model, te, dev)
        LOG.info("eval: epoch=%d top1=%.2f", ep, acc)
        state = {"epoch": ep, "model": model.state_dict(), "opt": opt.state_dict(), "acc": acc}
        path = save_ckpt(state, args.ckpt_dir, ep)
        LOG.info("checkpoint: %s", path)

if __name__ == "__main__":
    main()

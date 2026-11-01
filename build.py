"""Builds template.yaml by embedding src/*.py as inline Lambda code and the saga ASL twice.
CloudFormation limits inline (ZipFile) code to 4096 characters per function."""
import pathlib, sys

root = pathlib.Path(__file__).parent
text = (root / "template.base.yaml").read_text()
ok = True


def indent(s, n):
    return "".join((" " * n + l if l.strip() else "\n") for l in s.splitlines(True)).rstrip("\n")


for name in ("gateway", "naive", "saga_reserve", "saga_pay", "saga_confirm", "saga_refund",
             "saga_release", "saga_start", "reconciler", "ledger", "batch"):
    code = (root / "src" / (name + ".py")).read_text().rstrip("\n") + "\n"
    print("%-14s %4d / 4096 chars" % (name, len(code)))
    ok &= len(code) <= 4096
    text = text.replace("__CODE_%s__" % name, indent(code, 8))
asl = (root / "src" / "saga.asl.yaml").read_text().rstrip("\n") + "\n"
text = text.replace("__ASL__", indent(asl, 8))
asl2 = (root / "src" / "recheck.asl.yaml").read_text().rstrip("\n") + "\n"
text = text.replace("__ASL_RECHECK__", indent(asl2, 8))
(root / "template.yaml").write_text(text)
sys.exit(0 if ok else 1)

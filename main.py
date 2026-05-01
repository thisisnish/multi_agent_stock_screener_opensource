# main.py — GCF gen2 source-deploy shim.
#
# GCF gen2 requires the entry-point function to be importable from a main.py
# at the root of --source.  This shim re-exports eval_handler so that the
# function can be declared with --entry-point=eval_handler while the real
# implementation lives in gcf/eval/main.py.
#
# The screener/ package is importable here because --source=. uploads the
# entire repo root, putting screener/ on sys.path alongside gcf/.

from gcf.eval.main import eval_handler  # noqa: F401

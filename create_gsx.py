import argparse
import pandas as pd
from src.emissions import run_sensitivity_module

parser = argparse.ArgumentParser(
    description="""
        _summary_
        """,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)

parser.add_argument(
    '-m', '--module',
    help="""
        """,
    type=str,
    default='sensitivity',
    metavar='\b',
)

parser.add_argument(
    '-i', '--input',
    help="""
        """,
    type=str,
    default="./",
    metavar="\b"
)

parser.add_argument(
    '-o', '--outdir',
    help='CSV output directory',
    default="./",
    metavar="\b"
)

args = parser.parse_args()

if __name__ == "__main__":

    if args.module == "sensitivity":
        run_sensitivity_module(
            plan_simulation_file=args.input,
            outdir=args.outdir,
        )

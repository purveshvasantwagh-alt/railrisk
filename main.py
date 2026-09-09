"""
main.py
-------
Root execution wrapper for running RailRisk directly from the project directory.
Usage:
    python main.py --train 12301 --target-station NDLS --prep-time 45
"""

import sys
from railrisk.cli import main

if __name__ == "__main__":
    sys.exit(main())
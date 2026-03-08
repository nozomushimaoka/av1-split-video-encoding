#!/usr/bin/env python3

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from av1_encoder.s3.cli import main

if __name__ == '__main__':
    exit(main())

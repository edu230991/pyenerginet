"""A simple script to append environment variables that are useful in CI-CD
"""

import os
import datetime as dt
from pyenerginet import __version__

env_file = os.getenv("GITHUB_ENV")
now_str = dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")

with open(env_file, "a") as myfile:
    myfile.write(f"VERSION_TAG=v{__version__}-{now_str}")

"""A simple script to append environment variables that are useful in CI-CD
"""

import os
from pyenerginet import __version__

env_file = os.getenv("GITHUB_ENV")

with open(env_file, "a") as myfile:
    myfile.write(f"VERSION_TAG={__version__}")

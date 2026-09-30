"""Test imports for the local modular VOLTTRON checkout.

The package dependencies are intentionally not installed from this repository's
Poetry metadata while running the focused tests.  That metadata permits a PyPI
VOLTTRON wheel, which can silently hide the source trees under test.  Put the
local source trees first instead.
"""

import sys
import platform
from pathlib import Path

from gevent import monkey
if not monkey.is_module_patched("subprocess"):
    monkey.patch_subprocess()

WORKSPACE = Path(__file__).parent.parent
LOCAL_SOURCE_TREES = (
    WORKSPACE / "volttron-core" / "src",
    WORKSPACE / "volttron-lib-zmq" / "src",
    WORKSPACE / "volttron-lib-auth" / "src",
    WORKSPACE / "volttron-lib-base-historian" / "src",
    Path(__file__).parent / "src",
)

for source_tree in reversed(LOCAL_SOURCE_TREES):
    if source_tree.is_dir():
        sys.path.insert(0, str(source_tree))


# VOLTTRON's topic constants inspect platform.uname()[1] at import time. In this
# container ``platform.uname()`` invokes a subprocess which can block while
# pytest is collecting; use os.uname() or static values instead.
platform.processor = lambda: ""
try:
    platform.uname = lambda: platform.uname_result("Linux", "localhost", "5.15.0", "1", "x86_64")
except Exception:
    pass

if hasattr(platform, "_syscmd_uname"):
    platform._syscmd_uname = lambda *args, **kwargs: ""


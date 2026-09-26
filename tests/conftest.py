import logging
import sys

import pytest


@pytest.fixture(autouse=True)
def _log_case(request):
    logging.getLogger("case").info("=== %s ===", request.node.nodeid)
    yield

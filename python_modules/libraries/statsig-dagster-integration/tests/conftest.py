import os

import pytest
from sqlalchemy.engine import make_url


@pytest.fixture
def postgres_url() -> str:
    url = os.getenv("DAEMON_FIXTURE_POSTGRES_URL")
    if not url:
        pytest.skip("Requires the disposable daemon Unix-socket Postgres harness")
    parsed = make_url(url)
    assert parsed.username == "daemon_fixture" and parsed.database == "daemon_fixture"
    assert parsed.host in {None, ""} and parsed.query["host"] == "/pgfixture/socket"
    return url

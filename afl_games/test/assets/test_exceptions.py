import pytest
from main.assets.exceptions import ResponseIsEmpty

@pytest.fixture(scope="session")
def params():
    return {"q": "games", "year": 2025}
@pytest.fixture(scope="session")
def e():
    raise ResponseIsEmpty(params)


def test_reponse_empty(params, e):
    error = e()
    assert error.params == params
"""conftest.py module for testing."""
import pytest
from scottbrian_algo1.algo_api import AlgoApp


@pytest.fixture(scope='session')
def algo_app() -> "AlgoApp":
    """Instantiate and return an AlgoApp for testing.

    Returns:
        An instance of AlgoApp
    """
    a_algo_app = AlgoApp()
    return a_algo_app

import pytest
import os
import datetime
from dotenv import load_dotenv
from main.connectors.squiggle_api import SquiggleApiClient

@pytest.fixture(scope="session")
def squiggle_client():
    load_dotenv()
    #squiggle user agent
    SQUIGGLE_USER_AGENT = os.environ.get("SQUIGGLE_USER_AGENT")
    return SquiggleApiClient(SQUIGGLE_USER_AGENT)

# Test structure response from 'games' API.
def test_games_response(squiggle_client):
    this_year = datetime.date.today().year
    games_data = squiggle_client.get_games(year = this_year)
    assert len(games_data) > 0
    assert isinstance(games_data, dict)
    assert "games" in games_data

# Test structure response from 'tips' API.
def test_odds_response(squiggle_client):
    this_year = datetime.date.today().year
    odds_data = squiggle_client.get_odds(year = this_year, round = 1)
    assert len(odds_data) > 0
    assert isinstance(odds_data, dict)
    assert "tips" in odds_data

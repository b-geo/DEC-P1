from extract import odds
import os
import dotenv


def test_odss_api():
    """Test"""
    dotenv.load_dotenv()
    ODDS_API_KEY = os.environ.get("ODDS_API_KEY")
    actual_data = odds.extract(ODDS_API_KEY)
    assert isinstance(actual_data, list)
    assert isinstance(actual_data[0], dict)
    print(actual_data)
    assert 'sport_key' in actual_data[0]

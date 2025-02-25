import requests

class SquiggleBaseClient:
    """ _summary_ A client to get AFL betting odds and games details from Squiggle.

    Note: An API Key is not required but the API does force a User-Agent in the headers that is not bot-like: https://api.squiggle.com.au/#section_bots
    
    Example:
    
    a = SquiggleBaseClient("email@email.com")
    b = a.get_odds(year="2025", round="0")
    """
    def __init__(self, user_agent: str):
        self._base_url = "https://api.squiggle.com.au/"
        if user_agent is None:
            raise Exception("User Agent for headers cannot be set to None.")
        self._user_agent = user_agent
    def get_games(self, year: str = "2025", round: str = "0") -> dict:
        """Fetches games for a specific season (year) and round.

        Args:
            year (str, optional): _description_. Defaults to "2025".
            round (str, optional): _description_. Defaults to "0".

        Raises:
            Exception:  _description_ Response was a status other than 200.

        Returns:
            dict:  _description_ Returns the API response as JSON. Format: {"tips": [{odds of game}]}
        """
        params = {"q": "games", "year": year, "round": round}
        headers = {"User-Agent": self._user_agent}
        response = requests.get(self._base_url,params = params, headers= headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to extract games from Squiggle API. Status Code: {response.status_code}. Response: {response.text}"
            )
    def get_odds(self, year: str = "2025", round: str = "0") -> dict:
        """Fetches odds for a specific season (year) and round.

        Args:
            year (str, optional): _description_. Defaults to "2025".
            round (str, optional): _description_. Defaults to "0".

        Raises:
            Exception:  _description_ Response was a status other than 200.

        Returns:
            dict:  _description_ Returns the API response as JSON. Format: {"games": [{game details}]}
        """
        params = {"q": "tips", "year": year, "round": round, "source": "5"}
        headers = {"User-Agent": self._user_agent}
        response = requests.get(self._base_url,params = params, headers= headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to extract odds from Squiggle API. Status Code: {response.status_code}. Response: {response.text}"
            )


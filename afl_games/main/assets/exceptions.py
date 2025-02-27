class SquiggleException:
    pass
#trying some custom exceptions for fun
class ResponseIsEmpty(SquiggleException):
    def __init__(self, params: dict):
        self.params = params
        super().__init__(
            f"Response with params: {params} was empty."
        )
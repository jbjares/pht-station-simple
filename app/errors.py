__all__ = ['startup_error_if']


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class StartupError(Error):
    """Exception raised for errors in the input.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str):
        self.message = message


def startup_error_if(test: bool, msg: str):
    if test:
        raise StartupError(msg)


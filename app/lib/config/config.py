__all__ = ['PHT_URI_STATION', 'PHT_URI_SERVICE']

import os
from ..functions import is_quoted
from ..functions import string_has_content
from ..errors import startup_error_if
from ..models.url import URL


################################################################################################
# How to extract general values from the config
################################################################################################
def _get_value_from_env(config_key: str):

    # Error if the config key does not exist in the environment
    startup_error_if(
        config_key not in os.environ,
        "Config Key: {} has not been found in the environment of the station application".format(config_key))

    value = os.environ[config_key].strip()

    # Error if the value for the config key does not have any non whitespace content
    startup_error_if(
        not string_has_content(value),
        "Value for config key: {} is empty".format(config_key))

    # Error if the value appears to be quoted (this could be a shell artifact)
    startup_error_if(
        is_quoted(value),
        "Value for config key: {} appears to be quoted. Please make sure that the environment is set up correctly!"
        .format(config_key))

    return value


################################################################################################
# How to extract proper URLs from the config
################################################################################################
def _get_url_value_from_env(config_key: str):

    url = URL(_get_value_from_env(config_key))

    startup_error_if(
        not url.has_scheme() or not url.has_netloc(),
        "URL for config key: {} either has no scheme or no netloc".format(config_key))
    return url


################################################################################################
# Expose the configuration values
################################################################################################
PHT_URI_STATION = _get_url_value_from_env('PHT_URI_STATION')
PHT_URI_SERVICE = _get_url_value_from_env('PHT_URI_SERVICE')

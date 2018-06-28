from urllib.parse import urlparse
from ..functions import string_has_content


# Represents an URI
class URL:
    def __init__(self, text: str):
        self.parseresult = urlparse(text)

    def has_scheme(self):
        return string_has_content(self.parseresult.scheme)

    def has_netloc(self):
        return string_has_content(self.parseresult.netloc)

    def has_path(self):
        return string_has_content(self.parseresult.path)

    def geturl(self):
        return self.parseresult.geturl()

    def get_port(self):
        if ':' in self.parseresult.netloc:
            return int(self.parseresult.netloc.split(':')[-1])
        return 80

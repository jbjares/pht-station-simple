__all__ = ['is_quoted', 'string_has_content', 'pprint']

import string
import random
import sys


def is_quoted(s: str):
    return (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'"))


def string_has_content(s: str):
    return s is not None and s.strip() != ''


def generate_random_string(length: int):
    chars = string.ascii_letters + string.punctuation + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def pprint(msg):
    print(msg, file=sys.stdout)
    print(msg, file=sys.stderr)
    sys.stderr.flush()
    sys.stdout.flush()


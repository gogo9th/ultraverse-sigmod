import random
import string

from .constants import C_ID


def NURand(A: int, C: int, x: int, y: int) -> int:
    return (((random.randint(0, A) | random.randint(x, y)) + C) % (y - x + 1)) + x


def random_astring(min_len: int, max_len: int) -> str:
    length = random.randint(min_len, max_len)
    return "".join(random.choice(string.ascii_letters) for _ in range(length))


def random_nstring(min_len: int, max_len: int) -> str:
    length = random.randint(min_len, max_len)
    return "".join(random.choice(string.digits) for _ in range(length))


def make_last_name(num: int) -> str:
    syllables = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"]
    return syllables[num // 100] + syllables[(num // 10) % 10] + syllables[num % 10]


def get_customer_id() -> int:
    return NURand(1023, C_ID, 1, 3000)

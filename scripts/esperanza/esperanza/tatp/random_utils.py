import random
import string

from .constants import START_TIME_SLOTS, SUB_NBR_LENGTH


def generate_sub_nbr(s_id: int) -> str:
    return f"{s_id:0{SUB_NBR_LENGTH}d}"


def random_s_id(rng: random.Random, num_subscribers: int) -> int:
    return rng.randint(1, num_subscribers)


def random_ai_type(rng: random.Random) -> int:
    return rng.randint(1, 4)


def random_sf_type(rng: random.Random) -> int:
    return rng.randint(1, 4)


def random_start_time(rng: random.Random) -> int:
    return rng.choice(START_TIME_SLOTS)


def random_location(rng: random.Random) -> int:
    # MySQL INTEGER is signed; keep within int32 range to avoid out-of-range inserts.
    return rng.randint(1, 0x7FFFFFFF)


def random_bit(rng: random.Random) -> int:
    return rng.randint(0, 1)


def random_hex(rng: random.Random) -> int:
    return rng.randint(0, 15)


def random_byte2(rng: random.Random) -> int:
    return rng.randint(0, 255)


def random_data(rng: random.Random) -> int:
    return rng.randint(0, 255)


def random_alpha_string(rng: random.Random, length: int) -> str:
    return "".join(rng.choice(string.ascii_letters) for _ in range(length))


def random_numberx(rng: random.Random) -> str:
    return "".join(rng.choice(string.digits) for _ in range(SUB_NBR_LENGTH))

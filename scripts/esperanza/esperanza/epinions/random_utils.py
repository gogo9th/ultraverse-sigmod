import math
import random
import string

FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325
FNV_PRIME_64 = 1099511628211


def fnv_hash_64(val: int) -> int:
    hashval = FNV_OFFSET_BASIS_64
    data = val & 0xFFFFFFFFFFFFFFFF
    for _ in range(8):
        octet = data & 0xFF
        data = data >> 8
        hashval = hashval ^ octet
        hashval = (hashval * FNV_PRIME_64) & 0xFFFFFFFFFFFFFFFF
    return abs(hashval)


class ZipfianGenerator:
    ZIPFIAN_CONSTANT = 0.99

    @staticmethod
    def zetastatic(n: int, theta: float) -> float:
        return sum(1.0 / math.pow(i + 1, theta) for i in range(n))

    def __init__(
        self,
        rng: random.Random,
        *args,
        zipfian_constant: float | None = None,
        zetan: float | None = None,
    ) -> None:
        if zipfian_constant is None:
            zipfian_constant = self.ZIPFIAN_CONSTANT

        if len(args) == 1:
            min_val = 0
            max_val = int(args[0]) - 1
        elif len(args) == 2:
            if isinstance(args[1], float):
                min_val = 0
                max_val = int(args[0]) - 1
                zipfian_constant = float(args[1])
            else:
                min_val = int(args[0])
                max_val = int(args[1])
        elif len(args) == 3:
            min_val = int(args[0])
            max_val = int(args[1])
            zipfian_constant = float(args[2])
        else:
            raise TypeError("expected (rng, items), (rng, min, max), or (rng, items, zipfian_constant)")

        self.rng = rng
        self.base = min_val
        self.items = max_val - min_val + 1
        if self.items <= 0:
            raise ValueError("items must be >= 1")

        self.theta = float(zipfian_constant)
        self.zeta2theta = self.zetastatic(2, self.theta)
        if zetan is None:
            self.zetan = self.zetastatic(self.items, self.theta)
        else:
            self.zetan = float(zetan)
        self.alpha = 1.0 / (1.0 - self.theta)
        self.eta = (1 - math.pow(2.0 / self.items, 1 - self.theta)) / (1 - self.zeta2theta / self.zetan)

    def nextLong(self) -> int:
        u = self.rng.random()
        uz = u * self.zetan

        if uz < 1.0:
            return self.base
        if uz < 1.0 + math.pow(0.5, self.theta):
            return self.base + 1

        ret = self.base + int(self.items * math.pow(self.eta * u - self.eta + 1, self.alpha))
        if ret > self.base + self.items - 1:
            return self.base + self.items - 1
        return ret

    def nextInt(self) -> int:
        return int(self.nextLong())


class ScrambledZipfianGenerator:
    ZETAN = 26.46902820178302
    ITEM_COUNT = 10000000000

    def __init__(self, rng: random.Random, *args) -> None:
        if len(args) == 1:
            min_val = 0
            max_val = int(args[0]) - 1
        elif len(args) == 2:
            min_val = int(args[0])
            max_val = int(args[1])
        else:
            raise TypeError("expected (rng, items) or (rng, min, max)")

        self.base = min_val
        self.items = max_val - min_val + 1
        if self.items <= 0:
            raise ValueError("items must be >= 1")

        self.zipfian = ZipfianGenerator(
            rng,
            self.ITEM_COUNT,
            zipfian_constant=ZipfianGenerator.ZIPFIAN_CONSTANT,
            zetan=self.ZETAN,
        )

    def nextLong(self) -> int:
        ret = self.zipfian.nextLong()
        return self.base + (fnv_hash_64(ret) % self.items)

    def nextInt(self) -> int:
        return int(self.nextLong())


def random_string(rng: random.Random, length: int) -> str:
    return "".join(rng.choice(string.ascii_letters) for _ in range(length))


def random_email(rng: random.Random) -> str:
    local = "".join(rng.choice(string.ascii_lowercase) for _ in range(10))
    domain = "".join(rng.choice(string.ascii_lowercase) for _ in range(8))
    return f"{local}@{domain}.com"

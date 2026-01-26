from __future__ import annotations

from typing import ClassVar


class CustomerId:
    def __init__(self, id: int, depart_airport_id: int, max_customer: int, max_airport: int) -> None:
        self.id = int(id)
        self.depart_airport_id = int(depart_airport_id)
        self.max_customer = int(max_customer)
        self.max_airport = int(max_airport)

        self._customer_digits = len(str(self.max_customer))
        self._airport_digits = len(str(self.max_airport))
        self._customer_padding = "0" * max(0, self._customer_digits - len(str(self.id)))
        self._airport_padding = "0" * max(0, self._airport_digits - len(str(self.depart_airport_id)))

    def encode(self) -> str:
        return f"{self._customer_padding}{self.id}{self._airport_padding}{self.depart_airport_id}"

    @classmethod
    def decode(cls, composite_id: str, max_customer: int, max_airport: int) -> CustomerId:
        customer_digits = len(str(max_customer))
        airport_digits = len(str(max_airport))
        expected_len = customer_digits + airport_digits
        if len(composite_id) < expected_len:
            raise ValueError("composite_id is shorter than expected")

        customer_segment = composite_id[:customer_digits]
        airport_segment = composite_id[customer_digits:expected_len]

        def _strip_leading_zeros(segment: str) -> str:
            result = ""
            started = False
            for ch in segment:
                if ch == "0" and not started:
                    continue
                started = True
                result += ch
            return result

        customer_str = _strip_leading_zeros(customer_segment)
        airport_str = _strip_leading_zeros(airport_segment)
        if not customer_str or not airport_str:
            raise ValueError("composite_id contains an empty component")

        return cls(int(customer_str), int(airport_str), max_customer, max_airport)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, CustomerId):
            return False
        return self.id == other.id and self.depart_airport_id == other.depart_airport_id

    def __hash__(self) -> int:
        return hash((self.id, self.depart_airport_id))

    def __repr__(self) -> str:
        return (
            f"CustomerId(id={self.id}, depart_airport_id={self.depart_airport_id}, "
            f"max_customer={self.max_customer}, max_airport={self.max_airport})"
        )


class FlightId:
    _DEPART_DATE_DIGITS: ClassVar[int] = 6

    def __init__(
        self,
        airline_id: int,
        depart_airport_id: int,
        arrive_airport_id: int,
        depart_date: int,
        num_airports: int,
    ) -> None:
        self.airline_id = int(airline_id)
        self.depart_airport_id = int(depart_airport_id)
        self.arrive_airport_id = int(arrive_airport_id)
        self.depart_date = int(depart_date)
        self.num_airports = int(num_airports)
        self._airport_digits = len(str(self.num_airports))

    def encode(self) -> str:
        depart_airport = str(self.depart_airport_id).zfill(self._airport_digits)
        arrive_airport = str(self.arrive_airport_id).zfill(self._airport_digits)
        depart_date = str(self.depart_date).zfill(self._DEPART_DATE_DIGITS)
        return f"{depart_airport}{arrive_airport}{depart_date}{self.airline_id}"

    @classmethod
    def decode(cls, composite_id: str, num_airports: int) -> FlightId:
        airport_digits = len(str(num_airports))
        fixed_len = airport_digits * 2 + cls._DEPART_DATE_DIGITS
        if len(composite_id) <= fixed_len:
            raise ValueError("composite_id is shorter than expected")

        depart_start = 0
        depart_end = airport_digits
        arrive_end = depart_end + airport_digits
        date_end = arrive_end + cls._DEPART_DATE_DIGITS

        depart_airport_str = composite_id[depart_start:depart_end]
        arrive_airport_str = composite_id[depart_end:arrive_end]
        depart_date_str = composite_id[arrive_end:date_end]
        airline_str = composite_id[date_end:]

        if not airline_str:
            raise ValueError("composite_id contains an empty airline id")

        return cls(
            int(airline_str),
            int(depart_airport_str),
            int(arrive_airport_str),
            int(depart_date_str),
            num_airports,
        )

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, FlightId):
            return False
        return (
            self.airline_id == other.airline_id
            and self.depart_airport_id == other.depart_airport_id
            and self.arrive_airport_id == other.arrive_airport_id
            and self.depart_date == other.depart_date
        )

    def __hash__(self) -> int:
        return hash((self.airline_id, self.depart_airport_id, self.arrive_airport_id, self.depart_date))

    def __repr__(self) -> str:
        return (
            "FlightId("
            f"airline_id={self.airline_id}, depart_airport_id={self.depart_airport_id}, "
            f"arrive_airport_id={self.arrive_airport_id}, depart_date={self.depart_date}, "
            f"num_airports={self.num_airports}"
            ")"
        )

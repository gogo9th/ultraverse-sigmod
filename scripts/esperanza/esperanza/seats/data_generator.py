from __future__ import annotations

import csv
import json
import math
import random
import string
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None

from .constants import (
    CUSTOMER_NUM_FREQUENTFLYERS_MAX,
    CUSTOMER_NUM_FREQUENTFLYERS_MIN,
    DATA_BATCH_SIZE,
    DEFAULT_NUM_CUSTOMERS,
    FLIGHTS_DAYS_FUTURE,
    FLIGHTS_DAYS_PAST,
    FLIGHTS_NUM_SEATS,
    FLIGHTS_PER_DAY_MAX,
    FLIGHTS_PER_DAY_MIN,
    FLIGHT_STATUS_OPEN,
    MAX_CUSTOMER_IATTR,
    MAX_CUSTOMER_SATTR,
    MAX_FLIGHT_IATTR,
    MAX_FREQUENTFLYER_IATTR,
    MAX_FREQUENTFLYER_SATTR,
    RESERVATION_PRICE_MAX,
    RESERVATION_PRICE_MIN,
)
from .encoding import CustomerId, FlightId


class ZipfianGenerator:
    ZIPFIAN_CONSTANT = 0.99

    @staticmethod
    def _zetastatic(n: int, theta: float) -> float:
        return sum(1.0 / math.pow(i + 1, theta) for i in range(n))

    def __init__(self, rng: random.Random, items: int, zipfian_constant: float | None = None) -> None:
        if items <= 0:
            raise ValueError("items must be >= 1")
        self.rng = rng
        self.base = 0
        self.items = int(items)
        if zipfian_constant is None:
            zipfian_constant = self.ZIPFIAN_CONSTANT
        self.theta = float(zipfian_constant)
        self.zeta2theta = self._zetastatic(2, self.theta)
        self.zetan = self._zetastatic(self.items, self.theta)
        self.alpha = 1.0 / (1.0 - self.theta)
        self.eta = (1 - math.pow(2.0 / self.items, 1 - self.theta)) / (1 - self.zeta2theta / self.zetan)

    def nextInt(self) -> int:
        u = self.rng.random()
        uz = u * self.zetan
        if uz < 1.0:
            return self.base
        if uz < 1.0 + math.pow(0.5, self.theta):
            return self.base + 1
        ret = self.base + int(self.items * math.pow(self.eta * u - self.eta + 1, self.alpha))
        if ret > self.base + self.items - 1:
            return self.base + self.items - 1
        return int(ret)


class SeatsDataGenerator:
    def __init__(self, conn, num_customers: int = DEFAULT_NUM_CUSTOMERS) -> None:
        self.conn = conn
        self.num_customers = int(num_customers)
        self.batch_size = DATA_BATCH_SIZE
        self.rng = random.Random()
        self.data_dir = Path(__file__).resolve().parent.parent.parent / "seats_data"

        self._country_code_to_id: dict[str, int] = {}
        self._airport_code_to_id: dict[str, int] = {}
        self._airline_code_to_id: dict[str, int] = {}

        self._airport_locations: dict[int, tuple[float | None, float | None]] = {}
        self._airport_ids: list[int] = []
        self._airline_ids: list[int] = []

        self._customers: list[tuple[str, str, int]] = []
        self._airport_customer_counts: list[int] = []

        self._flight_start_date: datetime | None = None
        self._flight_upcoming_date: datetime | None = None

    def generate_all(self) -> None:
        country_total = self._count_csv_rows(self.data_dir / "table.country.csv")
        airport_total = self._count_csv_rows(self.data_dir / "table.airport.csv")
        airline_total = self._count_csv_rows(self.data_dir / "table.airline.csv")

        countries_bar = self._new_progress(country_total, "country")
        airports_bar = self._new_progress(airport_total, "airport")
        airlines_bar = self._new_progress(airline_total, "airline")
        distances_bar = self._new_progress(None, "airport_distance")
        customers_bar = self._new_progress(self.num_customers, "customer")
        flights_bar = self._new_progress(None, "flight")
        frequent_bar = self._new_progress(None, "frequent_flyer")
        profile_bar = self._new_progress(1, "config_profile")

        try:
            self.load_countries(countries_bar)
            self.load_airports(airports_bar)
            self.load_airlines(airlines_bar)
            self.generate_airport_distances(distances_bar)
            self.generate_customers(customers_bar)
            self.generate_flights(flights_bar)
            self.generate_frequent_flyers(frequent_bar)
            self.save_config_profile(profile_bar)
        finally:
            self._close_progress(countries_bar)
            self._close_progress(airports_bar)
            self._close_progress(airlines_bar)
            self._close_progress(distances_bar)
            self._close_progress(customers_bar)
            self._close_progress(flights_bar)
            self._close_progress(frequent_bar)
            self._close_progress(profile_bar)

    def load_countries(self, progress: object | None = None) -> None:
        sql = "INSERT INTO country (co_id, co_name, co_code_2, co_code_3) VALUES (%s, %s, %s, %s)"
        path = self.data_dir / "table.country.csv"

        def rows() -> Iterable[tuple]:
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for idx, row in enumerate(reader):
                    name = row["Name"].strip()
                    code2 = row["Code2"].strip()
                    code3 = row["Code3"].strip()
                    self._country_code_to_id[code3] = idx
                    yield (idx, name, code2, code3)

        self._execute_batches(sql, rows(), progress=progress)

    def load_airports(self, progress: object | None = None) -> None:
        iattr_cols = [f"ap_iattr{i:02d}" for i in range(16)]
        columns = [
            "ap_id",
            "ap_code",
            "ap_name",
            "ap_city",
            "ap_postal_code",
            "ap_co_id",
            "ap_longitude",
            "ap_latitude",
            "ap_gmt_offset",
            "ap_wac",
            *iattr_cols,
        ]
        sql = self._make_insert_sql("airport", columns)
        path = self.data_dir / "table.airport.csv"

        def rows() -> Iterable[tuple]:
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for idx, row in enumerate(reader):
                    code = row["Code"].strip()
                    name = row["Name"].strip()
                    city = row["City"].strip()
                    postal = row["Postal_Code"].strip() or None
                    country_code = row["Country_Code"].strip()
                    co_id = self._country_code_to_id.get(country_code)
                    if co_id is None:
                        raise ValueError(f"Unknown country code '{country_code}' for airport {code}")
                    longitude = self._parse_float(row["Longitude"])
                    latitude = self._parse_float(row["Latitude"])
                    gmt_offset = self._parse_float(row["Gmt_Offset"])
                    wac = self._parse_int(row["World_Area_Code"])
                    self._airport_code_to_id[code] = idx
                    self._airport_locations[idx] = (latitude, longitude)
                    self._airport_ids.append(idx)
                    iattrs = self._random_iattrs(16)
                    yield (
                        idx,
                        code,
                        name,
                        city,
                        postal,
                        co_id,
                        longitude,
                        latitude,
                        gmt_offset,
                        wac,
                        *iattrs,
                    )

        self._execute_batches(sql, rows(), progress=progress)

    def load_airlines(self, progress: object | None = None) -> None:
        iattr_cols = [f"al_iattr{i:02d}" for i in range(16)]
        columns = [
            "al_id",
            "al_iata_code",
            "al_icao_code",
            "al_call_sign",
            "al_name",
            "al_co_id",
            *iattr_cols,
        ]
        sql = self._make_insert_sql("airline", columns)
        path = self.data_dir / "table.airline.csv"

        def rows() -> Iterable[tuple]:
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for idx, row in enumerate(reader):
                    iata = row["Iata_Code"].strip() or None
                    icao = row["Icao_Code"].strip() or None
                    call_sign = row["Call_Sign"].strip() or None
                    name = row["Name"].strip()
                    country_code = row["Country_Code"].strip()
                    co_id = self._country_code_to_id.get(country_code)
                    if co_id is None:
                        raise ValueError(f"Unknown country code '{country_code}' for airline {name}")
                    if iata:
                        self._airline_code_to_id[iata] = idx
                    self._airline_ids.append(idx)
                    iattrs = self._random_iattrs(16)
                    yield (
                        idx,
                        iata,
                        icao,
                        call_sign,
                        name,
                        co_id,
                        *iattrs,
                    )

        self._execute_batches(sql, rows(), progress=progress)

    def generate_airport_distances(self, progress: object | None = None) -> None:
        sql = "INSERT INTO airport_distance (d_ap_id0, d_ap_id1, d_distance) VALUES (%s, %s, %s)"
        airports = [
            (ap_id, lat, lon)
            for ap_id, (lat, lon) in sorted(self._airport_locations.items())
            if lat is not None and lon is not None
        ]

        def rows() -> Iterable[tuple]:
            for i in range(len(airports) - 1):
                ap_id0, lat0, lon0 = airports[i]
                for j in range(i + 1, len(airports)):
                    ap_id1, lat1, lon1 = airports[j]
                    distance = self._haversine_miles(lat0, lon0, lat1, lon1)
                    if 0 < distance <= 100:
                        yield (ap_id0, ap_id1, distance)

        self._execute_batches(sql, rows(), progress=progress)

    def generate_customers(self, progress: object | None = None) -> None:
        if not self._airport_ids:
            raise RuntimeError("load_airports must be called before generate_customers")

        columns = [
            "c_id",
            "c_id_str",
            "c_base_ap_id",
            "c_balance",
            *[f"c_sattr{i:02d}" for i in range(MAX_CUSTOMER_SATTR)],
            *[f"c_iattr{i:02d}" for i in range(MAX_CUSTOMER_IATTR)],
        ]
        sql = self._make_insert_sql("customer2", columns)
        num_airports = len(self._airport_ids)
        zipf = ZipfianGenerator(self.rng, num_airports)
        self._airport_customer_counts = [0 for _ in range(num_airports)]
        self._customers.clear()

        def rows() -> Iterable[tuple]:
            for customer_num in range(self.num_customers):
                base_airport_id = zipf.nextInt()
                self._airport_customer_counts[base_airport_id] += 1
                airport_customer_id = self._airport_customer_counts[base_airport_id] - 1
                customer_id = CustomerId(
                    airport_customer_id,
                    base_airport_id,
                    self.num_customers,
                    num_airports,
                ).encode()
                customer_id_str = f"C_{customer_num}_{base_airport_id}"
                balance = self.rng.uniform(0, 10000)
                sattrs = [self._random_string(32)]
                sattrs.extend(self._random_string(8) for _ in range(MAX_CUSTOMER_SATTR - 1))
                iattrs = self._random_iattrs(MAX_CUSTOMER_IATTR)
                self._customers.append((customer_id, customer_id_str, base_airport_id))
                yield (
                    customer_id,
                    customer_id_str,
                    base_airport_id,
                    balance,
                    *sattrs,
                    *iattrs,
                )

        self._execute_batches(sql, rows(), progress=progress)

    def generate_flights(self, progress: object | None = None) -> None:
        if not self._airport_ids or not self._airline_ids:
            raise RuntimeError("load_airports/load_airlines must be called before generate_flights")

        columns = [
            "f_id",
            "f_al_id",
            "f_depart_ap_id",
            "f_depart_time",
            "f_arrive_ap_id",
            "f_arrive_time",
            "f_status",
            "f_base_price",
            "f_seats_total",
            "f_seats_left",
            *[f"f_iattr{i:02d}" for i in range(MAX_FLIGHT_IATTR)],
        ]
        sql = self._make_insert_sql("flight", columns)

        start_date = datetime.now() - timedelta(days=FLIGHTS_DAYS_PAST)
        upcoming_date = datetime.now()
        self._flight_start_date = start_date
        self._flight_upcoming_date = upcoming_date

        num_airports = len(self._airport_ids)
        end_date = upcoming_date + timedelta(days=FLIGHTS_DAYS_FUTURE)

        def rows() -> Iterable[tuple]:
            day = start_date
            while day <= end_date:
                daily_ids: set[str] = set()
                flights_today = self.rng.randint(FLIGHTS_PER_DAY_MIN, FLIGHTS_PER_DAY_MAX)
                for _ in range(flights_today):
                    while True:
                        depart_airport = self.rng.choice(self._airport_ids)
                        arrive_airport = self.rng.choice(self._airport_ids)
                        if arrive_airport == depart_airport:
                            continue
                        airline_id = self.rng.choice(self._airline_ids)
                        depart_time = day + timedelta(minutes=self.rng.randint(0, 24 * 60 - 1))
                        depart_hours = int((depart_time - start_date).total_seconds() // 3600)
                        flight_id = FlightId(
                            airline_id,
                            depart_airport,
                            arrive_airport,
                            depart_hours,
                            num_airports,
                        ).encode()
                        if flight_id in daily_ids:
                            continue
                        daily_ids.add(flight_id)
                        arrive_time = self._estimate_arrival_time(
                            depart_airport,
                            arrive_airport,
                            depart_time,
                        )
                        base_price = self.rng.uniform(RESERVATION_PRICE_MIN, RESERVATION_PRICE_MAX)
                        iattrs = self._random_iattrs(MAX_FLIGHT_IATTR)
                        yield (
                            flight_id,
                            airline_id,
                            depart_airport,
                            depart_time,
                            arrive_airport,
                            arrive_time,
                            FLIGHT_STATUS_OPEN,
                            base_price,
                            FLIGHTS_NUM_SEATS,
                            FLIGHTS_NUM_SEATS,
                            *iattrs,
                        )
                        break
                day = day + timedelta(days=1)

        self._execute_batches(sql, rows(), progress=progress)

    def generate_frequent_flyers(self, progress: object | None = None) -> None:
        if not self._customers or not self._airline_ids:
            raise RuntimeError("generate_customers and load_airlines must be called before generate_frequent_flyers")

        columns = [
            "ff_c_id",
            "ff_al_id",
            "ff_c_id_str",
            *[f"ff_sattr{i:02d}" for i in range(MAX_FREQUENTFLYER_SATTR)],
            *[f"ff_iattr{i:02d}" for i in range(MAX_FREQUENTFLYER_IATTR)],
        ]
        sql = self._make_insert_sql("frequent_flyer", columns)

        def rows() -> Iterable[tuple]:
            for customer_id, customer_id_str, _ in self._customers:
                ff_count = self.rng.randint(CUSTOMER_NUM_FREQUENTFLYERS_MIN, CUSTOMER_NUM_FREQUENTFLYERS_MAX)
                if ff_count <= 0:
                    continue
                ff_count = min(ff_count, len(self._airline_ids))
                airline_ids = self.rng.sample(self._airline_ids, ff_count)
                for airline_id in airline_ids:
                    sattrs = [self._random_string(32) for _ in range(MAX_FREQUENTFLYER_SATTR)]
                    iattrs = self._random_iattrs(MAX_FREQUENTFLYER_IATTR)
                    yield (
                        customer_id,
                        airline_id,
                        customer_id_str,
                        *sattrs,
                        *iattrs,
                    )

        self._execute_batches(sql, rows(), progress=progress)

    def save_config_profile(self, progress: object | None = None) -> None:
        columns = [
            "cfp_scale_factor",
            "cfp_aiport_max_customer",
            "cfp_flight_start",
            "cfp_flight_upcoming",
            "cfp_flight_past_days",
            "cfp_flight_future_days",
            "cfp_flight_offset",
            "cfp_reservation_offset",
            "cfp_num_reservations",
            "cfp_code_ids_xrefs",
        ]
        sql = self._make_insert_sql("config_profile", columns)
        scale_factor = self.num_customers / float(DEFAULT_NUM_CUSTOMERS)
        airport_max_customer = {
            str(airport_id): count for airport_id, count in enumerate(self._airport_customer_counts)
        }
        code_id_xrefs = {
            "co_id": self._country_code_to_id,
            "ap_id": self._airport_code_to_id,
            "al_id": self._airline_code_to_id,
        }
        flight_start = self._flight_start_date or datetime.now()
        flight_upcoming = self._flight_upcoming_date or datetime.now()
        row = (
            scale_factor,
            json.dumps(airport_max_customer, ensure_ascii=True),
            flight_start,
            flight_upcoming,
            FLIGHTS_DAYS_PAST,
            FLIGHTS_DAYS_FUTURE,
            None,
            None,
            0,
            json.dumps(code_id_xrefs, ensure_ascii=True),
        )
        self._execute_batches(sql, [row], progress=progress)

    def _execute_batches(
        self,
        sql: str,
        rows: Iterable[tuple],
        progress: object | None = None,
    ) -> None:
        cursor = self.conn.cursor()
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) >= self.batch_size:
                cursor.executemany(sql, batch)
                self.conn.commit()
                if progress is not None:
                    progress.update(len(batch))
                batch.clear()
        if batch:
            cursor.executemany(sql, batch)
            self.conn.commit()
            if progress is not None:
                progress.update(len(batch))
        cursor.close()

    def _new_progress(self, total: int | None, desc: str) -> object | None:
        if tqdm is None:
            return None
        return tqdm(
            total=total,
            desc=desc,
            unit="rows",
            ascii=True,
            dynamic_ncols=True,
            mininterval=0.5,
            disable=not sys.stderr.isatty(),
        )

    def _close_progress(self, progress: object | None) -> None:
        if progress is not None:
            progress.close()

    @staticmethod
    def _make_insert_sql(table: str, columns: list[str]) -> str:
        cols = ", ".join(columns)
        values = ", ".join(["%s"] * len(columns))
        return f"INSERT INTO {table} ({cols}) VALUES ({values})"

    @staticmethod
    def _count_csv_rows(path: Path) -> int:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            return sum(1 for _ in reader)

    @staticmethod
    def _parse_float(value: str) -> float | None:
        val = value.strip()
        if not val:
            return None
        return float(val)

    @staticmethod
    def _parse_int(value: str) -> int | None:
        val = value.strip()
        if not val:
            return None
        return int(float(val))

    def _random_string(self, length: int) -> str:
        return "".join(self.rng.choice(string.ascii_letters) for _ in range(length))

    def _random_iattrs(self, count: int) -> list[int]:
        return [self.rng.randint(0, 1 << 30) for _ in range(count)]

    @staticmethod
    def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        radius = 3959.0
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        c = 2 * math.asin(min(1.0, math.sqrt(a)))
        return radius * c

    def _estimate_arrival_time(self, depart_airport: int, arrive_airport: int, depart_time: datetime) -> datetime:
        duration_hours = None
        depart_loc = self._airport_locations.get(depart_airport)
        arrive_loc = self._airport_locations.get(arrive_airport)
        if depart_loc and arrive_loc:
            lat1, lon1 = depart_loc
            lat2, lon2 = arrive_loc
            if lat1 is not None and lon1 is not None and lat2 is not None and lon2 is not None:
                distance = self._haversine_miles(lat1, lon1, lat2, lon2)
                if distance > 0:
                    duration_hours = max(1.0, distance / 500.0)
        if duration_hours is None:
            duration_hours = self.rng.uniform(1.0, 6.0)
        return depart_time + timedelta(hours=duration_hours)

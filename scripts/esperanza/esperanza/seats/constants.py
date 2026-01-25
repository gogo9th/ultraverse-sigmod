# SEATS Benchmark Constants
# Based on ultraverse-benchbase SEATSConstants.java

# Flight configuration
FLIGHTS_NUM_SEATS = 150
FLIGHTS_FIRST_CLASS_OFFSET = 10
FLIGHTS_DAYS_PAST = 1
FLIGHTS_DAYS_FUTURE = 50
FLIGHTS_PER_DAY_MIN = 1125
FLIGHTS_PER_DAY_MAX = 1875

# Airport configuration (loaded from CSV, this is the default limit)
NUM_AIRPORTS = 9263  # Total airports in table.airport.csv

# Customer configuration
DEFAULT_NUM_CUSTOMERS = 100000
CUSTOMER_NUM_FREQUENTFLYERS_MIN = 0
CUSTOMER_NUM_FREQUENTFLYERS_MAX = 10

# Reservation configuration
RESERVATION_PRICE_MIN = 100.0
RESERVATION_PRICE_MAX = 1000.0

# Workload configuration
DEFAULT_QUERY_COUNT = 1000000

# Transaction weights (sum = 100)
TRANSACTION_WEIGHTS = {
    "find_flights": 10,
    "find_open_seats": 35,
    "new_reservation": 20,
    "update_reservation": 15,
    "update_customer": 10,
    "delete_reservation": 10,
}

# Probabilities (percentages)
PROB_FIND_FLIGHTS_NEARBY_AIRPORT = 25
PROB_FIND_FLIGHTS_RANDOM_AIRPORTS = 10
PROB_DELETE_WITH_CUSTOMER_ID_STR = 20
PROB_UPDATE_WITH_CUSTOMER_ID_STR = 20
PROB_DELETE_WITH_FREQUENTFLYER_ID_STR = 20
PROB_SINGLE_FLIGHT_RESERVATION = 10
PROB_UPDATE_FREQUENT_FLYER = 25
PROB_REQUEUE_DELETED_RESERVATION = 90

# Distance thresholds for nearby airport search (miles)
DISTANCES = [5, 10, 25, 50, 100]

# Flight status codes
FLIGHT_STATUS_OPEN = 0
FLIGHT_STATUS_CLOSED = 1

# Maximum attribute indices
MAX_CUSTOMER_SATTR = 20
MAX_CUSTOMER_IATTR = 20
MAX_FLIGHT_IATTR = 30
MAX_RESERVATION_IATTR = 9
MAX_FREQUENTFLYER_SATTR = 4
MAX_FREQUENTFLYER_IATTR = 16

# Batch sizes for data generation
DATA_BATCH_SIZE = 1000

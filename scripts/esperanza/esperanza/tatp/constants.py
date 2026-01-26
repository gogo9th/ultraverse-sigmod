# Data generation constants
DEFAULT_NUM_SUBSCRIBERS = 100000
DEFAULT_QUERY_COUNT = 100000

# Transaction weights (ultraverse-benchbase)
TRANSACTION_WEIGHTS = {
    'get_access_data': 35,
    'get_subscriber_data': 35,
    'get_new_destination': 10,
    'update_location': 14,
    'delete_call_forwarding': 2,
    'insert_call_forwarding': 2,
    'update_subscriber_data': 2,
}

START_TIME_SLOTS = [0, 8, 16]
SUB_NBR_LENGTH = 15

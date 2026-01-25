# Data generation constants
NUM_USERS = 2000  # Number of baseline Users
NUM_ITEMS = 1000  # Number of baseline pages

NAME_LENGTH = 24
EMAIL_LENGTH = 24
TITLE_LENGTH = 128
DESCRIPTION_LENGTH = 512
COMMENT_LENGTH = 256
COMMENT_MIN_LENGTH = 32

NUM_REVIEWS = 500  # average reviews per item
NUM_TRUST = 200  # average trusts per user

# Default query count
DEFAULT_QUERY_COUNT = 100000

# Transaction weights (BenchBase standard)
TRANSACTION_WEIGHTS = {
    'get_review_item_by_id': 10,
    'get_reviews_by_user': 10,
    'get_avg_rating_by_trusted_user': 10,
    'get_item_avg_rating': 10,
    'get_item_reviews_by_trusted_user': 10,
    'update_user_name': 12,
    'update_item_title': 13,
    'update_review_rating': 17,
    'update_trust_rating': 8,
}

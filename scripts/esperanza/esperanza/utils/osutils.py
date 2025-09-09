import os
import sys

def get_current_user() -> str:
    """
    returns the current user's name.
    """
    return os.environ['USER']
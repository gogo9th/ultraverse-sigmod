#!/usr/bin/env python
import os
import sys
import random

if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'zabacus.settings')
    os.environ['DJANGO_SECRET_KEY'] = ''.join(
        [random.SystemRandom().choice('abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*(-_=+)') for i in range(50)])
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)

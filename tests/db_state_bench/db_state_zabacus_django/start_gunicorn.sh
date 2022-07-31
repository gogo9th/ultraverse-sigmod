#!/bin/bash
gunicorn --access-logfile - --workers 3 --bind unix:/var/run/zabacus-api/gunicorn.sock zabacus.wsgi:application


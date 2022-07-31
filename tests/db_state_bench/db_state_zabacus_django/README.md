# ZAbacus Django-Graphene

GraphQL backend for ZAbacus application.

## Requirements

Currently using Python 3.6.6. See `requirements.txt` for Python packages.

On Debian/Ubuntu you will probably need to install some mysqlclient devel headers. You will also need the Python development headers.

```bash
sudo apt install python3.6-dev default-libmysqlclient-dev
```

```bash
python3.6 -m venv venv
pip install -r requirements.txt
```

## Test run

```
./manage.py runserver
```

## Deployment notes

You must create an environment variable `$DJANGO_SECRET_KEY` to run the server in deployment mode.

You must also use an environment variable `$RECAPTCHA_SECRET_KEY` for reCaptcha to work correctly on the production site.
The default reCaptcha secret key only works for localhost.

MySQL will be used in deployment mode. Credentials should be supplied in `zabacus/settings.py`.

If you want to use the Django admin interface, generate static files by running

```bash
./manage.py collectstatic
```

Static files by default go to `/var/www/zabacus-api/static/`, make sure you can write to that directory.  
Configure nginx to serve `api.zabacus.org/static` from the static file directory.

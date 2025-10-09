# Tracking the C3G projects
[![Test suite](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml/badge.svg?branch=main)](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml) on main branch  
[![Test suite](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml/badge.svg?branch=dev)](https://github.com/c3g/project_tracking/actions/workflows/PyTest.yml) on dev branch

This is an API to access and modify the C3G data processing tracking database.

## Install
We recommend using postgress in producton, but the project is fully compatible with sqlite.
We also publish container on GitHub reposityory (ghcr.io) and test or system using podman.
### From GitHub with sqlite (best for developer):
Sqlite needs to be installed on your machine.
Here, you will deploy a development instance of the app and be able to modify the code in the repo with auto-reload 
```bash
git clone  git@github.com:c3g/project_tracking.git
cd project_tracking
git checkout dev # If you are developing from the dev branch!
python -m venv venv
source ./venv/bin/activate
pip install --upgrade pip
pip install -e  .
# Setting the db url is optional, the default will be in the app installation folder
export C3G_SQLALCHEMY_DATABASE_URI="sqlite:////tmp/my_test_db.sql"
# initialyse the db
flask --app project_tracking init-db
# run the app
flask --app project_tracking --debug run
```

By default, falsk will run the server on  http://127.0.0.1:5000. You can test that everything 
is fine with curl in a terminal:
```bash
$ curl http://127.0.0.1:5000/
Welcome to the TechDev tracking API!
# The help api is also available. It lists all the server urls.
$ curl http://127.0.0.1:5000/help
----------
URL:
        /
DOC:
        Welcome page

[...]
```



Once the server is running, you can still initialise the database, you can even flush it clear of any entry with:

```bash
# WARNING this will erase all entry to you Database!
flask  --app project_tracking init-db --flush --db-uri "sqlite:////tmp/my_test_db.sql"
```

### Using podman and sqlite:
We have a [ghcr.io repo for the project](https://github.com/c3g/project_tracking/pkgs/container/project_tracking)
There are version releases and `latest` tag relates to the latest release.
```bash
SQLITE_DB_FOLDER=<folder on host with WR access>
podman run -v $SQLITE_DB_FOLDER:/sqlite:Z -it --rm -p 8000:8000 -e C3G_INIT_DB=1 ghcr.io/c3g/project_tracking:latest
```
The app runs on port 8000 inside the container, the `-e C3G_INIT_DB=1` option will the db in 
`$SQLITE_DB_FOLDER/tracking_db.sql`. 



### From GitHub using postgress:
postgress needs to be installed with a database  names <DB_NAME>,
assessible by user <POSTGRESS_USER>, with a pasword <POSTGRESS_PW>

```bash
git clone  git@github.com:c3g/project_tracking.git
cd project_tracking
python -m venv venv
source ./venv/bin/activate
pip install .[postgres]
C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://<POSTGRESS_USER>:<POSTGRESS_PW>@<POSTGRESS_HOST>/<DB_NAME>?client_encoding=utf8"
gunicorn -w 4 'project_tracking:create_app()'
````

### Using podman and postgress:
Here we expect postgres to be listening to the localhost (127.0.0.1) interface. 
The podman option `--network slirp4netns:allow_host_loopback=true` 
options makes it so that the host `127.0.0.1` interface is
reachable with the `10.0.2.2` adress inside the container. That is why the C3G_SQLALCHEMY_DATABASE_URI
is set to that value.
```bash
podman pull ghcr.io/c3g/project_tracking:latest
export C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://<POSTGRESS_USER>:<POSTGRESS_PW>@10.0.2.2/<POSTGRESS_DB_NAME>?client_encoding=utf8"
podman secret create --env C3G_SQLALCHEMY_DATABASE_URI C3G_SQLALCHEMY_DATABASE_URI
podman run --secret C3G_SQLALCHEMY_DATABASE_URI,type=env -p 8000:8000 -e C3G_INIT_DB=1 --network slirp4netns:allow_host_loopback=true ghcr.io/c3g/project_tracking:latest
```

## Install from pypi:
```
No release yet
```

### Run tests
Once you have modified the code, you can run the test to make sure you have not broken anything. In the git repo:
```bash
pip install -e  .[tests]
pytest -v
```







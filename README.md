# Tracking the C3G projects

This is an API to access and modify the C3G data processing tracking database.


## Setup a dev env with sqlite:
sqlite needs to be installed on your machine.
Here, you will deploy a development instance of the app and be able to modify the code in the repo with auto-reload 
```bash
git clone  git@github.com:c3g/project_tracking.git
git checkout dev # If you are developing from the dev branch!
cd project_tracking
python -m venv venv
source ./venv/bin/activate
pip install  --upgrade pip
pip install -e  .
# Seting the db url is optiopnal, the default will be in the app installation folder
export C3G_SQLALCHEMY_DATABASE_URI="sqlite:////tmp/my_test_db.sql" 
# initialyse the db
flask  --app project_tracking init-db
# run the app 
flask --app project_tracking --debug run
```

Once the server is running, you can still initialise the database, you can even flush it clear of any entry with 

```bash
# WARNING this will erase all entry to you Database!
flask  --app project_tracking init-db --flush --db_uri "sqlite:////tmp/my_test_db.sql" 

```

### Run tests
Once you have modified the code, you can run the test to make sure you have not broken anything. In the git repo:
```bash
pip install -e  .[tests]
pytest -v
```

### Container
We have a [quay.io repo for the project](https://quay.io/repository/c3genomics/project_tracking)
There are a dev release and version releases. The `latest` tag relates to the latest release.
```
SQLITE_DB_FOLDER=<folder on host with WR access>
podman run -v $SQLITE_DB_FOLDER:/sqlite:Z -it --rm -p 8000:8000 -e C3G_INIT_DB=1 quay.io/c3genomics/project_tracking:dev
```
The app runs on port 8000 inside the container, the `-e C3G_INIT_DB=1` option will the db in 
`$SQLITE_DB_FOLDER/tracking_db.sql`. 



##  Prod env with Postgress:

Use the latest commit from the tip of dev:
```bash
git clone  git@github.com:c3g/project_tracking.git
cd project_tracking
python -m venv venv
source ./venv/bin/activate
pip install .[postgres]
C3G_SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://postgres:toto@localhost/c3g_track?client_encoding=utf8" 
gunicorn -w 4 'project_tracking:create_app()'
````

From pypi:
```
No release yet
```




# c3g_project_tracking


This is an API to access and modify the C3G data processing tracking database.


## Install
```
pip install c3g_project_tracking
```
## Run

In dev:
```
    flask 'c3g_project_tracking:create_app()'
```
In prod:
```
    pip install gunicorn
    gunicorn -w 4 'c3g_project_tracking:create_app()'
```

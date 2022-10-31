import logging
import os

from flask import Flask

from . import api
from . import db



def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    logging.basicConfig(level=logging.DEBUG,
                        format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    app.register_blueprint(api.bp)
    db.init_app(app)
    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    return app
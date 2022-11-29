from project_tracking import create_app


def test_config():
    assert not create_app().testing
    assert create_app({'TESTING': True}).testing


def test_root(client):
    response = client.get('/')
    assert response.data == b'Welcome to the TechDev tracking API!'
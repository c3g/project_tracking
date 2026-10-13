import datetime
from project_tracking import model


def test_add_model(not_app_db, pre_filled_model):

    entry_dict, model_dict = pre_filled_model

    with not_app_db as db:
        db.add(model_dict['re1'])
        db.add(model_dict['re1'])
        db.commit()

    assert not_app_db.query(model.Project).one().name == entry_dict["project_name"]
    assert not_app_db.query(model.Specimen).one().name == entry_dict["specimen_name"]
    assert not_app_db.query(model.Operation).one().name == entry_dict["op_name"]
    m1 = not_app_db.query(model.Metric).first()
    r1 = m1.readsets[0]
    assert m1.name == entry_dict["metric_name"]
    assert r1.name == entry_dict["re1_name"]
    assert r1.metrics[0].value == entry_dict["me1_value"]


def test_location_from_uri_reuses_deleted_location(not_app_db):
    """uri is unique, so a soft-deleted Location must be returned instead of inserting a duplicate."""
    with not_app_db as db:
        file = model.File(name="sample.bam", type="bam")
        location = model.Location(uri="abacus:///path/sample.bam", endpoint="abacus", file=file, deleted=True)
        db.add_all([file, location])
        db.flush()

        found, warning = model.Location.from_uri(uri="abacus:///path/sample.bam", file=file, session=db)

        assert found.id == location.id
        assert warning is not None

import datetime
from sqlalchemy.orm import relationship, sessionmaker
from project_tracking import database, model


def test_add_model(not_app_db):

    project_name = 'Conglomerat of Good Health'
    project = model.Project(name=project_name)
    op_config_version = 0.1
    op_config_name = 'generic_index'
    op_c = model.OperationConfig(name=op_config_name, version=op_config_version)
    op_name = 'ingest'
    op = model.Operation(name=op_name,
                         status=model.StatusEnum.DONE,
                         operation_config=op_c,
                         project=project)

    sequencing_technology = 'Fancy Buzzword'
    exp = model.Experiment(sequencing_technology=sequencing_technology)
    pa_name = "P_O"
    pa = model.Patient(name=pa_name, project=project)
    sa_name = 'gros_bobo'
    sa = model.Sample(name=sa_name, patient=pa)
    ru_name = "cure the Conglomerat old director's partner 01"
    # breakpoint()
    instrument = 'Grosse machine du 6e'
    ru = model.Run(instrument=instrument, name=ru_name)
    re1_name = 'goble_goble'
    re2_name = 'goble_dable'
    re1 = model.Readset(name=re1_name, sample=sa, experiment=exp, run=ru)
    re2 = model.Readset(name=re2_name, sample=sa, experiment=exp, run=ru)
    job1 = model.Job(operation=op, status=model.StatusEnum.DONE, readset=[re1])
    job2 = model.Job(operation=op, status=model.StatusEnum.DONE, readset=[re2])
    me1_value = 'SHALLOW'
    me2_value = 'PRRETTY DEEP'
    metric_name = 'trucmuche'
    metric1 = model.Metric(value=me1_value,job=job1, name=metric_name, readset=[re1])
    metric2 = model.Metric(value=me2_value,job=job2, name=metric_name, readset=[re2])
    b1_uri = "beluga://project/rrg-bourqueg/MOH/RAW/data"
    b2_uri = "beluga://project/rrg-bourqueg/MOH/PROCESS/data"
    bundle1 = model.Bundle(uri=b1_uri)
    bundle2 = model.Bundle(uri=b2_uri)
    file1 = model.File(content='my.fastq', bundle=bundle1)
    file2 = model.File(content='*', bundle=bundle2) # do we want that?

    with not_app_db as db:
        db.add(re1)
        db.add(re2)
        db.commit()

    assert not_app_db.query(model.Project).one().name == project_name
    assert not_app_db.query(model.Patient).one().name == pa_name
    assert not_app_db.query(model.Operation).one().name == op_name
    m1 = not_app_db.query(model.Metric).first()
    r1 = m1.readset[0]
    assert m1.name == metric_name
    assert r1.name == re1_name
    assert r1.metric[0].value == me1_value


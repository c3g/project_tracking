from sqlalchemy import select
from project_tracking import database, model


def test_serialization(not_app_db, ingestion_json):

    project_name = 'Conglomerat of Good Health'
    op_config_version = 0.1
    op_config_name = 'generic_index'
    op_name = 'ingest'
    sequencing_technology = 'Fancy Buzzword'
    pa_name = "P_O"
    sa_name = 'gros_bobo'
    ru_name = "cure the Conglomerat old director's partner 01"
    instrument = 'Grosse machine du 6e'
    re1_name = 'goble_goble'
    re2_name = 'goble_dable'
    me1_value = 'SHALLOW'
    me2_value = 'PRRETTY DEEP'
    metric_name = 'trucmuche'
    b1_uri = "beluga://project/rrg-bourqueg/MOH/RAW/data"
    b2_uri = "beluga://project/rrg-bourqueg/MOH/PROCESS/data"

    op_c = model.OperationConfig(name=op_config_name, version=op_config_version)
    project = model.Project(name=project_name)
    op = model.Operation(name=op_name,
                         status=model.StatusEnum.DONE,
                         operation_config=op_c,
                         project=project)

    exp = model.Experiment(sequencing_technology=sequencing_technology)
    pa = model.Patient(name=pa_name, project=project)
    sa = model.Sample(name=sa_name, patient=pa)
    ru = model.Run(instrument=instrument, name=ru_name)
    re1 = model.Readset(name=re1_name, sample=sa, experiment=exp, run=ru)
    re2 = model.Readset(name=re2_name, sample=sa, experiment=exp, run=ru)
    job1 = model.Job(operation=op, status=model.StatusEnum.DONE, readset=[re1])
    job2 = model.Job(operation=op, status=model.StatusEnum.DONE, readset=[re2])
    metric1 = model.Metric(value=me1_value,job=job1, name=metric_name, readset=[re1])
    metric2 = model.Metric(value=me2_value,job=job2, name=metric_name, readset=[re2])
    bundle1 = model.Bundle(uri=b1_uri)
    bundle2 = model.Bundle(uri=b2_uri)
    bundle3 = model.Bundle(reference=[bundle2, bundle1])
    bundle4 = model.Bundle(reference=[bundle3])
    bundle5 = model.Bundle(reference=[bundle4, bundle3])
    file1 = model.File(content='my.fastq', bundle=bundle1)
    file2 = model.File(content='*', bundle=bundle2) # do we want that?


    with not_app_db as db:
        db.add(re1)
        db.add(re2)
        db.add(bundle5)
        db.commit()


    p = not_app_db.scalar(select(model.Project))
    readset = not_app_db.scalars(select(model.Readset)).first()
    bundles = not_app_db.scalars(select(model.Bundle)).all()
    jobs = not_app_db.scalars(select(model.Job)).all()

    assert isinstance(p.dumps, str)  # minimal
    assert isinstance(readset.dumps, str)   # join reference
    for b in bundles:
        assert isinstance(b.dumps, str)  # Auto reference
    for j in jobs:
        assert isinstance(j.dumps, str)  # enum type

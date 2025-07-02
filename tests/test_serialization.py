from sqlalchemy import select
from project_tracking import model


def test_serialization(not_app_db):

    project_name = 'Conglomerat of Good Health'
    op_config_version = 0.1
    op_config_name = 'generic_index'
    op_name = 'ingest'
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
                         status=model.StatusEnum.COMPLETED,
                         operation_config=op_c,
                         project=project)

    exp = model.Experiment(nucleic_acid_type=model.NucleicAcidTypeEnum.DNA)
    pa = model.Specimen(name=pa_name, project=project)
    sa = model.Sample(name=sa_name, specimen=pa)
    ru = model.Run(instrument=instrument, name=ru_name)
    re1 = model.Readset(name=re1_name, sample=sa, experiment=exp, run=ru)
    re2 = model.Readset(name=re2_name, sample=sa, experiment=exp, run=ru)
    job1 = model.Job(operation=op, status=model.StatusEnum.COMPLETED, readsets=[re1])
    job2 = model.Job(operation=op, status=model.StatusEnum.COMPLETED, readsets=[re2])
    metric1 = model.Metric(value=me1_value, job=job1, name=metric_name, readsets=[re1])
    metric2 = model.Metric(value=me2_value, job=job2, name=metric_name, readsets=[re2])
    file1 = model.File(name='my.fastq', readsets=[re1], jobs=[job1])
    location1 = model.Location.from_uri(uri=b1_uri+'/my.fastq', file=file1, session=not_app_db)
    location2 = model.Location.from_uri(uri=b2_uri+'/my.fastq', file=file1, session=not_app_db)




    with not_app_db as db:
        db.add(re1)
        db.add(re2)
        db.commit()


    p = not_app_db.scalar(select(model.Project))
    readset = not_app_db.scalars(select(model.Readset)).first()
    locations = not_app_db.scalars(select(model.Location)).all()
    jobs = not_app_db.scalars(select(model.Job)).all()
    files = not_app_db.scalars(select(model.File)).all()

    assert isinstance(p.dumps, str)  # minimal
    assert isinstance(readset.dumps, str)   # join reference
    for l in locations:
        assert isinstance(l.dumps, str)  # Auto reference
    for j in jobs:
        assert isinstance(j.dumps, str)  # enum type
    for f in files:
        assert isinstance(f.dumps, str)  # also dump location

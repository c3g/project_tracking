from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from marshmallow import fields

class BaseTableSchema(SQLAlchemyAutoSchema):
    class Meta:
        from .model import BaseTable
        model = BaseTable
        load_instance = True
        include_fk = True
        include_relationships = False

    creation = fields.DateTime()
    modification = fields.DateTime()
    extra_metadata = fields.Dict()

class ProjectSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Project
        model = Project

class SpecimenSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Specimen
        model = Specimen

class SampleSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Sample
        model = Sample

class ExperimentSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Experiment
        model = Experiment

class RunSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Run
        model = Run

class ReadsetSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Readset
        model = Readset

class OperationSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Operation
        model = Operation

class ReferenceSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Reference
        model = Reference

class OperationConfigSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import OperationConfig
        model = OperationConfig

class JobSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Job
        model = Job

class MetricSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Metric
        model = Metric

class LocationSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import Location
        model = Location

class FileSchema(BaseTableSchema):
    class Meta(BaseTableSchema.Meta):
        from .model import File
        model = File

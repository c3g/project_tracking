"""schema.py: Marshmallow schemas for serializing and deserializing SQLAlchemy models."""

import enum

from marshmallow import fields, post_dump
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy.orm.exc import DetachedInstanceError

from .model import (
    Project,
    Specimen,
    Sample,
    Readset,
    Experiment,
    Operation,
    Job,
    Metric,
    File,
    Location,
    OperationConfig,
    Run
    )

class EnumResolvingMixin:
    """
    Mixin to resolve enum attributes to their values during serialization.
    """
    @post_dump(pass_original=True)
    def resolve_enums(self, data, original, **kwargs):
        """
        Convert enum attributes to their values in the serialized output.
        """
        for attr in dir(original):
            if attr.startswith('_'):
                continue
            try:
                value = getattr(original, attr)
                if isinstance(value, enum.Enum):
                    data[attr] = value.value
            except (AttributeError, DetachedInstanceError):
                continue
        return data

class BaseSchema(EnumResolvingMixin, SQLAlchemyAutoSchema):
    """
    Base schema with dynamic relationship inclusion and enum resolution.
    """
    def __init__(self, *args, include_relationships=True, **kwargs):
        self.context = kwargs.pop("context", {})
        self.include_relationships = include_relationships

        # Dynamically set include_relationships in Meta
        if hasattr(self.Meta, 'include_relationships'):
            self.Meta.include_relationships = include_relationships
        else:
            setattr(self.Meta, 'include_relationships', include_relationships)

        super().__init__(*args, **kwargs)

    @post_dump(pass_original=True)
    def add_tablename(self, data, original, **kwargs):
        """
        Add __tablename__ to the serialized output.
        """
        if hasattr(original, '__tablename__'):
            data['tablename'] = original.__tablename__
        return data

def create_project_schema(include_relationships=True, **kwargs):
    """
    ProjectSchema factory function to create schema with or without relationships.
    """
    class ProjectSchema(BaseSchema):
        """
        ProjectSchema
        """
        class Meta:
            """
            ProjectSchema Meta
            """
            model = Project
            load_instance = True
            include_fk = True

    return ProjectSchema(**kwargs)

def create_specimen_schema(include_relationships=True, **kwargs):
    """
    SpecimenSchema factory function to create schema with or without relationships.
    """
    class SpecimenSchema(BaseSchema):
        """
        SpecimenSchema with optional relationship-like fields
        """
        class Meta:
            """
            SpecimenSchema Meta
            """
            model = Specimen
            load_instance = True
            include_fk = True

        if include_relationships:
            sample_ids = fields.Method("get_sample_ids")
            readset_ids = fields.Method("get_readset_ids")

        def get_sample_ids(self, obj):
            """
            Get associated sample IDs.
            """
            return obj.sample_ids

        def get_readset_ids(self, obj):
            """
            Get associated readset IDs.
            """
            return obj.readset_ids

    return SpecimenSchema(**kwargs)

def create_sample_schema(include_relationships=True, **kwargs):
    """
    SampleSchema factory function to create schema with or without relationships.
    """
    class SampleSchema(BaseSchema):
        """
        SampleSchema with optional relationship-like fields
        """
        class Meta:
            """
            SampleSchema Meta
            """
            model = Sample
            load_instance = True
            include_fk = True

        if include_relationships:
            readset_ids = fields.Method("get_readset_ids")

        def get_readset_ids(self, obj):
            """
            Get associated readset IDs.
            """
            return obj.readset_ids

    return SampleSchema(**kwargs)

def create_experiment_schema(include_relationships=True, **kwargs):
    """
    ExperimentSchema factory function to create schema with or without relationships.
    """
    class ExperimentSchema(BaseSchema):
        """
        ExperimentSchema
        """
        class Meta:
            """
            ExperimentSchema Meta
            """
            model = Experiment
            load_instance = True
            include_fk = True

    return ExperimentSchema(**kwargs)

def create_run_schema(include_relationships=True, **kwargs):
    """
    RunSchema factory function to create schema with or without relationships.
    """
    class RunSchema(BaseSchema):
        """
        RunSchema
        """
        class Meta:
            """
            RunSchema Meta
            """
            model = Experiment
            load_instance = True
            include_fk = True

    return RunSchema(**kwargs)

def create_readset_schema(include_relationships=True, **kwargs):
    """
    ReadsetSchema factory function to create schema with or without relationships.
    """
    class ReadsetSchema(BaseSchema):
        """
        ReadsetSchema
        """
        class Meta:
            """
            ReadsetSchema Meta
            """
            model = Readset
            load_instance = True
            include_fk = True

        if include_relationships:
            specimen_id = fields.Method("get_specimen_id")

        def get_specimen_id(self, obj):
            """
            Get associated specimen ID.
            """
            return obj.specimen_id

    return ReadsetSchema(**kwargs)

def create_operation_schema(include_relationships=True, **kwargs):
    """
    OperationSchema factory function to create schema with or without relationships.
    """
    class OperationSchema(BaseSchema):
        """
        OperationSchema
        """
        class Meta:
            """
            OperationSchema Meta
            """
            model = Operation
            load_instance = True
            include_fk = True

        if include_relationships:
            readset_ids = fields.Method("get_readset_ids")

        def get_readset_ids(self, obj):
            """
            Get associated readset IDs.
            """
            return obj.readset_ids

    return OperationSchema(**kwargs)

def create_operation_config_schema(include_relationships=True, **kwargs):
    """
    OperationConfigSchema factory function to create schema with or without relationships.
    """
    class OperationConfigSchema(BaseSchema):
        """
        OperationConfigSchema
        """
        class Meta:
            """
            OperationConfigSchema Meta
            """
            model = Operation
            load_instance = True
            include_fk = True

    return OperationConfigSchema(**kwargs)

def create_job_schema(include_relationships=True, **kwargs):
    """
    JobSchema factory function to create schema with or without relationships.
    """
    class JobSchema(BaseSchema):
        """
        JobSchema
        """
        class Meta:
            """
            JobSchema Meta
            """
            model = Job
            load_instance = True
            include_fk = True

        if include_relationships:
            readset_ids = fields.Method("get_readset_ids")

        def get_readset_ids(self, obj):
            """
            Get associated readset IDs.
            """
            # Not used for now but if the property readset_ids is too slow use this as it might be faster
            # return Job.get_readset_ids(self.context["session"], obj.id)
            return obj.readset_ids

    return JobSchema(**kwargs)

def create_metric_schema(include_relationships=True, **kwargs):
    """
    MetricSchema factory function to create schema with or without relationships.
    """
    class MetricSchema(BaseSchema):
        """
        MetricSchema
        """
        class Meta:
            """
            MetricSchema Meta
            """
            model = Metric
            load_instance = True
            include_fk = True

        if include_relationships:
            readset_ids = fields.Method("get_readset_ids")
            sample_ids = fields.Method("get_sample_ids")
            specimen_ids = fields.Method("get_specimen_ids")

        def get_readset_ids(self, obj):
            """
            Get associated readset IDs.
            """
            return obj.readset_ids

        def get_sample_ids(self, obj):
            """
            Get associated sample IDs.
            """
            return obj.sample_ids

        def get_specimen_ids(self, obj):
            """
            Get associated specimen IDs.
            """
            return obj.specimen_ids

    return MetricSchema(**kwargs)

def create_location_schema(include_relationships=True, **kwargs):
    """
    LocationSchema factory function to create schema with or without relationships.
    """
    class LocationSchema(BaseSchema):
        """
        LocationSchema
        """
        class Meta:
            """
            LocationSchema Meta
            """
            model = Location
            load_instance = True
            include_fk = True

    return LocationSchema(**kwargs)

def create_file_schema(include_relationships=True, **kwargs):
    """
    FileSchema factory function to create schema with or without relationships.
    """
    class FileSchema(BaseSchema):
        """
        FileSchema
        """
        class Meta:
            """
            FileSchema Meta
            """
            model = File
            load_instance = True
            include_fk = True

        if include_relationships:
            sample_ids = fields.Method("get_sample_ids")
            specimen_ids = fields.Method("get_specimen_ids")

        def get_sample_ids(self, obj):
            """
            Get associated sample IDs.
            """
            return obj.sample_ids

        def get_specimen_ids(self, obj):
            """
            Get associated specimen IDs.
            """
            return obj.specimen_ids

    return FileSchema(**kwargs)

SCHEMA_FACTORIES = {
    Project: create_project_schema,
    Specimen: create_specimen_schema,
    Sample: create_sample_schema,
    Experiment: create_experiment_schema,
    Run: create_run_schema,
    Readset: create_readset_schema,
    Operation: create_operation_schema,
    OperationConfig: create_operation_config_schema,
    Job: create_job_schema,
    Metric: create_metric_schema,
    Location: create_location_schema,
    File: create_file_schema
}

def serialize(obj, include_relationships=False, context=None):
    """
    Serialize a SQLAlchemy model instance or list of instances using the appropriate Marshmallow schema.

    Args:
        obj: A SQLAlchemy model instance or a list of instances.
        include_relationships: Whether to include relationship fields.
        context: Optional context dictionary for the schema.

    Returns:
        dict or list of dicts: Serialized representation.
    """
    if isinstance(obj, list):
        if not obj:
            return []
        model_cls = type(obj[0])
        schema_factory = SCHEMA_FACTORIES.get(model_cls)
        if not schema_factory:
            raise ValueError(f"No schema factory found for type {model_cls}")
        schema = schema_factory(
            many=True,
            include_relationships=include_relationships,
            context=context or {}
        )
        return schema.dump(obj)
    # implicit else as there is a return in the if so no need for an explicit one
    model_cls = type(obj)
    schema_factory = SCHEMA_FACTORIES.get(model_cls)
    if not schema_factory:
        raise ValueError(f"No schema factory found for type {model_cls}")
    schema = schema_factory(
        many=False,
        include_relationships=include_relationships,
        context=context or {}
    )
    return schema.dump(obj)

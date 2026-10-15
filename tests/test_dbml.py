"""The committed database diagram must follow the models."""
from pathlib import Path

from project_tracking.dbml import STAGES, enum_name, pipeline_columns, to_dbml, to_dot
from project_tracking.model import Base, LaneEnum, NucleicAcidTypeEnum

DBML = Path(__file__).resolve().parent.parent / "docs" / "database.dbml"


def test_committed_dbml_matches_models():
    assert DBML.read_text(encoding="utf-8") == to_dbml(), (
        "docs/database.dbml is out of date with the models: run utils/update_database_diagram.sh and commit both files."
    )


def test_dbml_content():
    dbml = to_dbml()
    assert enum_name(NucleicAcidTypeEnum) == "nucleic_acid_type"
    assert (
        "Table sample_group {\n  id integer [PK]\n"
        "  specimen_id integer [ref: > specimen.id, not null]\n  name text [unique, not null]"
    ) in dbml
    assert "  sample_id integer [PK, ref: > sample.id]" in dbml
    assert "  alias list\n" in dbml and "// extra_metadata json\n" in dbml
    assert "  lane lane\n" in dbml and f"  ONE [note: '{LaneEnum.ONE.value}']" in dbml
    assert "Table job_metric" not in dbml


def test_every_table_is_in_a_pipeline_stage():
    """A new table must be placed in a stage of project_tracking/dbml.py, or it's drawn in an "Other" box."""
    staged = {name for _, names in STAGES for name in names}
    assert set(Base.metadata.tables) - staged == set(), "add the new tables to STAGES in project_tracking/dbml.py"


def test_dot_is_a_left_to_right_pipeline():
    dot = to_dot()
    assert "rankdir=LR" in dot
    # Arrows go from the parent's key to the child's foreign key
    assert '"sample_group":"id_out":e -> "sample_group_member":"sample_group_id_in":w;' in dot
    assert '"specimen":"id_out":e -> "sample":"specimen_id_in":w;' in dot
    columns = pipeline_columns(STAGES, Base.metadata.tables)
    assert columns["project"] < columns["specimen"] < columns["sample"] < columns["readset"] < columns["operation"]
    assert columns["job"] < columns["metric"] and columns["file"] < columns["location"]

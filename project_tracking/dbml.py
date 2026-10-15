"""
Database diagram generated from the models so it follows every change: DBML (dbdiagram.io), and a Graphviz DOT
graph laid out as a pipeline, from the project on the left to the analysis outputs on the right
"""
import enum
import html
import re

import click
from sqlalchemy import Boolean, DateTime, Enum, Integer, JSON, LargeBinary, String

from . import model

COMMON_COLUMNS = ("deprecated", "deleted", "creation", "modification", "extra_metadata", "ext_id", "ext_src")
TYPES = ((Boolean, "boolean"), (DateTime, "timestamp"), (Integer, "integer"), (LargeBinary, "bytes"), (String, "text"))


def enum_name(enum_class):
    """
    DBML name of a model enum: LaneEnum -> lane, NucleicAcidTypeEnum -> nucleic_acid_type.
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", enum_class.__name__.removesuffix("Enum")).lower()


def column_type(column):
    """
    DBML type of a column: the enum name for enums, list or json for JSON columns (by their default).
    """
    if isinstance(column.type, Enum):
        return enum_name(column.type.enum_class)
    if isinstance(column.type, JSON):
        default = getattr(column.default, "arg", None)
        return "list" if getattr(default, "__name__", None) == "list" else "json"
    for sql_type, name in TYPES:
        if isinstance(column.type, sql_type):
            return name
    return column.type.__class__.__name__.lower()


def column_line(column):
    settings = []
    if column.primary_key:
        settings.append("PK")
    settings.extend(f"ref: > {fk.column.table.name}.{fk.column.name}" for fk in sorted(column.foreign_keys, key=str))
    if column.unique:
        settings.append("unique")
    if not column.nullable and not column.primary_key:
        settings.append("not null")
    suffix = f" [{', '.join(settings)}]" if settings else ""
    return f"  {column.name} {column_type(column)}{suffix}"


def table_block(table):
    """
    A table with its own columns (the common ones are listed once at the top), id first, then its indexes.
    """
    has_common = all(name in table.columns for name in COMMON_COLUMNS)
    columns = [c for c in table.columns if not (has_common and c.name in COMMON_COLUMNS)]
    columns.sort(key=lambda c: c.name != "id")
    lines = [f"Table {table.name} {{"] + [column_line(c) for c in columns]
    indexes = sorted(table.indexes, key=lambda i: i.name)
    if indexes:
        lines += ["", "  indexes {"]
        for index in indexes:
            names = [c.name for c in index.columns]
            target = names[0] if len(names) == 1 else f"({', '.join(names)})"
            lines.append(f"    {target} [name: '{index.name}'{', unique' if index.unique else ''}]")
        lines.append("  }")
    return "\n".join(lines + ["}"])


def enum_block(enum_class):
    """
    An enum with its member names, and their stored value as a note when it differs (e.g. lane ONE is '1').
    """
    lines = [f"Enum {enum_name(enum_class)} {{"]
    for member in enum_class:
        note = f" [note: '{member.value}']" if member.value != member.name else ""
        lines.append(f"  {member.name}{note}")
    return "\n".join(lines + ["}"])


def ordered_tables(metadata):
    """
    Entity tables in the order the models declare them, each link table right after the last table it links.
    """
    ordered = [cls.__table__ for cls in model.BaseTable.__subclasses__() if cls.__table__.name in metadata.tables]
    links = [table for table in metadata.tables.values() if table not in ordered]
    for link in sorted(links, key=lambda t: t.name):
        position = max(ordered.index(fk.column.table) for fk in link.foreign_keys)
        ordered.insert(position + 1, link)
    return ordered


def model_enums():
    """
    The enums of the models, in declaration order.
    """
    return [
        value for value in vars(model).values()
        if isinstance(value, type) and issubclass(value, enum.Enum) and value.__module__ == model.__name__
    ]


def to_dbml(metadata=model.Base.metadata):
    """
    The DBML of every table and enum of the models.
    """
    enums = model_enums()
    header = [
        "// Generated from project_tracking/model.py by 'flask --app project_tracking dbml': don't edit by hand.",
        "// Paste it in https://dbdiagram.io to see the diagram.",
        "//",
        "// Common fields to all tables but the link tables:",
    ] + [f"// {name} {column_type(model.Project.__table__.columns[name])}" for name in COMMON_COLUMNS]
    blocks = ["\n".join(header)]
    blocks += [table_block(table) for table in ordered_tables(metadata)]
    blocks += [enum_block(enum_class) for enum_class in enums]
    return "\n\n".join(blocks) + "\n"


# Pipeline stages of the diagram, left to right. A table in no stage is drawn in an "Other" box: add it to a stage.
STAGES = (
    ("Project and samples", ("project", "specimen", "sample", "sample_group", "sample_group_member")),
    ("Sequencing", ("experiment", "run", "readset")),
    ("Processing", ("operation_config", "reference", "operation", "readset_operation", "job", "readset_job")),
    ("Outputs", ("file", "location", "readset_file", "job_file", "metric", "readset_metric")),
)

# Colours of the previous dbdiagram rendering
DARK, BLUE, GREY, WHITE = "#29235c", "#1d71b8", "#e7e2dd", "#ffffff"


def _cell(text, **attributes):
    attributes = "".join(f' {key.upper()}="{value}"' for key, value in attributes.items())
    return f"<TD{attributes}>{text}</TD>"


def _header(text, colour, colspan=1):
    title = f'<FONT COLOR="{WHITE}"><B>{text}</B></FONT>'
    return f"<TR>{_cell(title, bgcolor=colour, colspan=colspan)}</TR>"


def table_node(table):
    """
    A table as an HTML-like Graphviz node: one row per column, with a port named after the column for the arrows.
    Primary keys are bold, NOT NULL columns marked (!); the common columns are left out, see the legend.
    """
    has_common = all(name in table.columns for name in COMMON_COLUMNS)
    columns = [c for c in table.columns if not (has_common and c.name in COMMON_COLUMNS)]
    columns.sort(key=lambda c: c.name != "id")
    rows = [_header(table.name, BLUE, colspan=2)]
    for column in columns:
        name = html.escape(column.name)
        name = f"<B>{name}</B>" if column.primary_key else name
        kind = html.escape(column_type(column)) + (" (!)" if not column.nullable and not column.primary_key else "")
        # Arrows arrive on the name (west) and leave from the type (east), so they never cross the text
        rows.append(
            f'<TR>{_cell(name, port=f"{column.name}_in", align="left", bgcolor=GREY)}'
            f'{_cell(f"<I>{kind}</I>", port=f"{column.name}_out", align="right", bgcolor=GREY)}</TR>'
        )
    label = f'<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="1" CELLPADDING="4">{"".join(rows)}</TABLE>'
    return f'    "{table.name}" [label=<{label}>];'


def enum_node(enum_class):
    """
    An enum of the legend, with each stored value when it differs from the member name.
    """
    rows = [_header(enum_name(enum_class), DARK)]
    for member in enum_class:
        value = f" = '{html.escape(str(member.value))}'" if member.value != member.name else ""
        rows.append(f'<TR>{_cell(f"<I>{member.name}{value}</I>", align="left", bgcolor=GREY)}</TR>')
    label = f'<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="1" CELLPADDING="4">{"".join(rows)}</TABLE>'
    return f'    "enum_{enum_name(enum_class)}" [label=<{label}>];'


def pipeline_columns(stages, tables):
    """
    Column of each table, left to right: a stage starts after the previous one, and within a stage a table comes one
    column after its parents of the same stage.
    """
    columns = {}
    start = 0
    for _, names in stages:
        names = [name for name in names if name in tables]
        depth = {}

        def local_depth(name, seen=()):
            if name not in depth:
                parents = {fk.column.table.name for fk in tables[name].foreign_keys}
                parents = [p for p in parents if p in names and p != name and p not in seen]
                depth[name] = 1 + max((local_depth(p, seen + (name,)) for p in parents), default=-1)
            return depth[name]

        for name in names:
            columns[name] = start + local_depth(name)
        if names:
            start = max(columns[name] for name in names) + 1
    return columns


def to_dot(metadata=model.Base.metadata):
    """
    Graphviz DOT of the database as a pipeline: stages left to right, an arrow from each referenced column to the
    foreign key referencing it (crow's foot on the many side), and a legend with the common columns and the enums.
    """
    tables = metadata.tables
    staged = [name for _, names in STAGES for name in names if name in tables]
    stages = list(STAGES) + [("Other", tuple(name for name in tables if name not in staged))]
    lines = [
        "// Generated from project_tracking/model.py by 'flask --app project_tracking dbml --format dot'.",
        "digraph database {",
        '  rankdir=LR; newrank=true; nodesep=0.4; ranksep=1.1; splines=spline; bgcolor="white";',
        f'  graph [fontname="helvetica", fontsize=20, fontcolor="{DARK}"];',
        f'  node [shape=plaintext, margin=0, fontname="helvetica", fontsize=14, fontcolor="{DARK}"];',
        f'  edge [color="{DARK}", penwidth=1.4, arrowhead=crow, arrowtail=none];',
    ]
    for index, (stage, names) in enumerate(stages):
        names = [name for name in names if name in tables]
        if not names:
            continue
        lines += [f"  subgraph cluster_{index} {{",
                  f'    label="{stage}"; style="rounded,filled"; color="{GREY}"; fillcolor="#f7f5f2";']
        lines += [table_node(tables[name]) for name in names]
        lines.append("  }")
    for table in tables.values():
        for fk in sorted(table.foreign_keys, key=lambda fk: fk.parent.name):
            parent = fk.column
            lines.append(f'  "{parent.table.name}":"{parent.name}_out":e -> "{table.name}":"{fk.parent.name}_in":w;')

    common = "".join(
        f'<TR>{_cell(name, align="left", bgcolor=GREY)}'
        f'{_cell(f"<I>{column_type(model.Project.__table__.columns[name])}</I>", align="right", bgcolor=GREY)}</TR>'
        for name in COMMON_COLUMNS
    )
    lines += [
        '  subgraph cluster_legend {',
        '    label="Legend: (!) NOT NULL, bold primary key, arrow from a key to the columns referencing it";',
        f'    style="rounded,filled"; color="{GREY}"; fillcolor="#f7f5f2";',
        f'    "common_columns" [label=<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="1" CELLPADDING="4">'
        f'{_header("every table but the link tables", DARK, colspan=2)}{common}</TABLE>>];',
    ]
    lines += [enum_node(enum_class) for enum_class in model_enums()]
    lines.append("  }")
    # One rank per pipeline column, so the stages read left to right
    columns = pipeline_columns(stages, tables)
    for column in sorted(set(columns.values())):
        names = "; ".join(f'"{name}"' for name in tables if columns[name] == column)
        lines.append(f"  {{rank=same; {names};}}")
    # The legend as a row along the first columns: chained invisibly from the first column
    legend = ["common_columns"] + [f"enum_{enum_name(e)}" for e in model_enums()]
    lines.append(f'  {{rank=same; "{staged[0]}"; "{legend[0]}";}}')
    lines.append("  " + " -> ".join(f'"{name}"' for name in legend) + " [style=invis];")
    lines.append("}")
    return "\n".join(lines) + "\n"


@click.command("dbml")
@click.option("--output", "-o", type=click.Path(dir_okay=False), help="File to write, standard output otherwise.")
@click.option("--format", "output_format", type=click.Choice(["dbml", "dot"]), default="dbml", show_default=True,
              help="DBML for dbdiagram.io, or Graphviz DOT laid out as a pipeline.")
def dbml_command(output=None, output_format="dbml"):
    """Print the diagram of the database generated from the models: DBML (dbdiagram.io) or Graphviz DOT."""
    text = to_dbml() if output_format == "dbml" else to_dot()
    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(text)
        click.echo(f"{output_format.upper()} written to {output}")
    else:
        click.echo(text, nl=False)

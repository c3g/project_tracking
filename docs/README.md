# Project Tracking

![Database diagram](./data/database.svg)

The diagram is generated from the models (`project_tracking/model.py`). It reads as a pipeline, left to
right: project and samples, sequencing, processing, then outputs (the stages are `STAGES` in
`project_tracking/dbml.py`; a new table must be added to one). Each arrow goes from a key to the columns
referencing it, with a crow's foot on the many side. `docs/database.dbml` describes the same database in
[DBML](https://dbml.dbdiagram.io/docs/): paste it in [dbdiagram.io](https://dbdiagram.io) to explore it
interactively.

After a model change, regenerate both from the repository root (the SVG needs Node.js):

```bash
utils/update_database_diagram.sh
```

`tests/test_dbml.py` fails while `docs/database.dbml` doesn't match the models or a table is in no stage, and
the Database Diagram workflow fails while either file is out of date.

# openGauss-sqlalchemy

OpenGauss dialect for SQLAlchemy.

This project has been tested with test suites of SQLAlchemy.


## Dependency for opengauss

- psycopg2 for opengauss

    Download and install reference
    > https://gitee.com/opengauss/openGauss-connector-python-psycopg2

    or

    > https://github.com/opengauss-mirror/openGauss-connector-python-psycopg2

### NOTE

Using unmatched psycopg2 may cause part of the autoescape test cases in
 SQLAlchemy version 2.0+ failed.

## Installation

```
# From pypi
>>> python -m pip install opengauss-sqlalchemy

# From source code
>>> python setup.py install
```

## Usage

Create an DSN(Data Source Name) that points to your OpenGauss database.

```
>>> import sqlalchemy as sa
# With centralized mode
>>> sa.create_engine('opengauss://username:password@host:port/database_name')
# Or
>>> sa.create_engine('opengauss+psycopg2://username:password@host:port/database_name')
# With distributed mode
>>> sa.create_engine('opengauss+dc_psycopg2://username:password@host:port/database_name')
# Or
>>> sa.create_engine('opengauss+dc_psycopg2://username:password@/database_name?host=hostA:portA&host=hostB:portB')
```
Version 2.4.0 introduces an asynchronous connection interface. Due to the specifics of asyncpg, when using asyncpg connections with the vector data type, you need to register this data type. A code example is provided below:
```
from opengauss_sqlalchemy.register_async import register_vector
engine = create_async_engine("opengauss+asyncpg://username:password@host:port/database_name")

@event.listens_for(engine.sync_engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.run_async(register_vector)
```

See the [OpenGauss DeveloperGuide](https://docs.opengauss.org/en/docs/3.1.0/docs/BriefTutorial/BriefTutorial.html) for more infomation.

## Features For Centralized and Distributed OpenGauss

### Index

- Index with `USING method`
```
tbl = Table("testtbl", m, Column("data", String))
Index("test_idx1", tbl.c.data, opengauss_using="btree")
```

- Index with column expression
```
tbl = Table(
    "testtbl",
    m,
    Column("data", String),
    Column("data2", Integer, key="d2"),
)

Index(
    "test_idx1",
    tbl.c.data,
    tbl.c.d2,
    opengauss_ops={"data": "text_pattern_ops", "d2": "int4_ops"},
)
```

- Index with `LOCAL`, only available for index on a partitioned table
```
tbl = Table(
    "testtbl",
    m,
    Column("data", Integer),
    opengauss_partition_by="RANGE (data) ..."
)
Index("test_idx1", tbl.c.data, opengauss_local=[""])

Index(
    "test_idx2",
    tbl.c.data,
    opengauss_local=[
        "PARTITION data_index1",
        "PARTITION data_index2 TABLESPACE example3",
    ]
)
```

- Index with `WITH`
```
tbl = Table("testtbl", m, Column("data", String))
Index("test_idx1", tbl.c.data, opengauss_with={"fillfactor": 50})
```

- Index with `TABLESPACE`
```
tbl = Table("testtbl", m, Column("data", String))
Index("test_idx1", tbl.c.data, opengauss_tablespace="sometablespace")
```

- Index with `WHERE`, unsupported for index on a partitioned table
```
tbl = Table("testtbl", m, Column("data", Integer))
Index(
    "test_idx1",
    tbl.c.data,
    opengauss_where=and_(tbl.c.data > 5, tbl.c.data < 10),
)
```

### Table

- Table with `WITH ({storage_parameter = value})`
```
Table("some_table", ..., opengauss_with={"storage_parameter": "value"})
```

- Table with `ON COMMIT`
```
Table("some_talbe", ..., prefixes=["TEMPORARY"], opengauss_on_commit="PRESERVE ROWS")
```

- Table with `COMPRESS`
```
Table("some_talbe", ..., opengauss_with={"ORIENTATION": "COLUMN"}, opengauss_compress=True)
```

- Table with `TABLESPACE tablespace_name`
```
Table("some_talbe", ..., opengauss_tablespace="tablespace_name")
```

- Table with `PARTITION BY`
```
Table("some_talbe", ..., opengauss_partition_by="RANGE(column_name) "
        "(PARTITION P1 VALUES LESS THAN(10), "
        "PARTITION P2 VALUES LESS THAN(MAXVALUE))")
```

- Table with `ENABLE ROW MOVEMENT`
```
Table("some_talbe", ..., opengauss_partition_by="RANGE(column_name) ...",
    opengauss_enable_row_movement=True)
```

### Vector Data Type
- vector data type with `select`
```
tbl = Table(
    "test",
    m,
    Column("id", Integer),
    Column("bit_embedding", BIT(3)),
    Column("sparsevec_embedding", SPARSEVEC(3)),
    Column("vector_embedding", VECTOR(3))
)

select(tbl.c.id).order_by(tbl.c.bit_embedding.jaccard_distance('110'))

select(tbl.c.id).order_by(tbl.c.sparsevec_embedding.l2_distance(SparseVector([1, 2, 3])))

select(tbl.c.id).order_by(tbl.c.vector_embedding.l2_distance([1,2,3]))
```

## Features For Centralized OpenGauss

### Index

- Index with `CONCURRENTLY`
```
tbl = Table("testtbl", m, Column("data", Integer))
Index("test_idx1", tbl.c.data, opengauss_concurrently=True)
```

## Features For Distributed OpenGauss

### TABLE

- Table with `DISTRIBUTE BY`
```
Table("some_table", ..., opengauss_distribute_by="HASH(column_name)")
```
NOTE: table without distributable columns will be set with "DISTRIBUTE BY REPLICATION"

- Table with `TO GROUP`
```
Table("some_table", ..., opengauss_to="GROUP group_name")
```


## Releasing

### Build python wheel
```
>>> python -m pip install wheel
>>> python setup.py bdist_wheel
```

### Testing

1. Set environment with `export LD_LIBRARY_PATH=` and `export PYTHONPATH=` to your path of package `psycopg2`.
2. Install opengauss and update configuration, see "Steps to install and config opengauss for testing".
3. Execute `tox -e py38`.


### Steps to install and config centralized opengauss for testing

1. Add OS user for opengauss ```>>> useradd omm -g dbgrp```
2. Change owner of opengauss dir ```>>> chown omm:dbgrp ${db_dir} -R```
3. Switch to user omm ```>>> su - omm```
4. Install opengauss ```>>> sh install.sh -w ${db_password} -p 37200```
5. Start opengauss ```>>> gs_ctl start -D ${db_dir}/data/single_node/```
6. Login opengauss ```>>> gsql -d postgres -p 37200```
7. Create database user and create database & schema for testing
```
openGauss=# create user scott identified by 'Tiger123';
openGauss=# create database test with owner=scott encoding='utf8' template=template0;
openGauss=# GRANT ALL PRIVILEGES TO scott;
openGauss=# ALTER DATABASE test SET default_text_search_config = 'pg_catalog.english';
openGauss=# \c test
test=# create schema test_schema AUTHORIZATION scott;
test=# create schema test_schema_2 AUTHORIZATION scott;
test=# \q
```
8. Config opengauss
```
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "ssl=off"
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "max_prepared_transactions = 100"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "local all scott sha256"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "host all scott 127.0.0.1/32 sha256"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "host all scott 0.0.0.0/0 sha256"
>>> gs_ctl stop -D ${db_dir}/data/single_node/
>>> gs_tl start -D ${db_dir}/data/single_node/
```
9. Optional: enable log_statement
```
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "log_min_error_statement = error"
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "log_statement = 'all'"
```

# openGauss-sqlalchemy

This is the openGauss dialect library that adapts to the SQLAlchemy framework.

The project has passed the test cases of the SQLAlchemy community.


## Runtime Dependencies

- `psycopg2 for opengauss`

    Download it from one of the following addresses and install it in the operating environment. Address 1:
    > https://gitcode.com/opengauss/openGauss-connector-python-psycopg2

    Address 2:

    > https://github.com/opengauss-mirror/openGauss-connector-python-psycopg2

### Notes

Incompatible versions of `psycopg2` may cause SQLAlchemy 2.0 autoescape test cases to fail.

To utilize autoescape features, ensure that openGauss is correctly installed and that `pg_config` points to the openGauss installation by modifying the `LD_LIBRARY_PATH` variable.

```
# Configure environment variables.
$ export GAUSSHOME=/home/omm/openGauss-server/mppdb_temp_install
$ export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH

$ which pg_config
# The expected result is as follows:
'/home/omm/openGauss-server/mppdb_temp_install/bin/pg_config'

# Delete psycopg2 for opengauss.
$ pip install --force-reinstall psycopg2

# If opengauss-sqlalchemy has been installed, reinstall it.
>>> python setup.py install
```

Alternatively, install `psycopg2-binary`.
```
$ pip uninstall psycopg2
$ pip install psycopg2-binary

# Change the value of `password_encryption_type` to `1` in the `postgresql.conf` file of the database instance.
$ vim /datanode/postgresql.conf

# Restart the database.
$ gs_ctl restart -D /datanode

Change the password.
$ gsql -d postgres -p <port_num>
openGauss=# alter user <usrname> with password <yourpassword>;
```


## Installation

```
# Install the library from PyPI.
>>> python -m pip install opengauss-sqlalchemy

# Install the library from the local source code.
>>> python setup.py install
```

## Usage Guide

Create a database in openGauss.

```
>>> import sqlalchemy as sa
# Access the database in centralized mode. Method 1:
>>> sa.create_engine('opengauss://username:password@host:port/database_name')
# Method 2:
>>> sa.create_engine('opengauss+psycopg2://username:password@host:port/database_name')
# Access the database in distributed mode. Method 1:
>>> sa.create_engine('opengauss+dc_psycopg2://username:password@host:port/database_name')
# Method 2:
>>> sa.create_engine('opengauss+dc_psycopg2://username:password@/database_name?host=hostA:portA&host=hostB:portB')
```

Version 2.4.0 introduces the asynchronous connection interface. Due to the specific nature of `asyncpg`, you must register the vector data type when using it. Refer to the code example below:
```
from opengauss_sqlalchemy.register_async import register_vector
engine = create_async_engine("opengauss+asyncpg://username:password@host:port/database_name")

# Bind an event listener.
@event.listens_for(engine.sync_engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.run_async(register_vector)
```

For details about the openGauss database development guide, see [openGauss Developer Guide](https://docs.opengauss.org/en/docs/latest/docs/Developerguide/Developerguide.html).

## Usage of openGauss Features (Centralized and Distributed)

### Indexing

- Indexing with the `USING` method
```
tbl = Table("testtbl", m, Column("data", String))
Index("test_idx1", tbl.c.data, opengauss_using="btree")
```

- Indexing with the `column` expression
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

- Indexing with `LOCAL`, only available for indexes on a partitioned table
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

- Indexing with `WITH`
```
tbl = Table("testtbl", m, Column("data", String))
Index("test_idx1", tbl.c.data, opengauss_with={"fillfactor": 50})
```

- Indexing with `TABLESPACE`
```
tbl = Table("testtbl", m, Column("data", String))
Index("test_idx1", tbl.c.data, opengauss_tablespace="sometablespace")
```

- Indexing with `WHERE`, unsupported for indexes on a partitioned table
```
tbl = Table("testtbl", m, Column("data", Integer))
Index(
    "test_idx1",
    tbl.c.data,
    opengauss_where=and_(tbl.c.data > 5, tbl.c.data < 10),
)
```

### Tables

- The table with `WITH ({storage_parameter = value})`
```
Table("some_table", ..., opengauss_with={"storage_parameter": "value"})
```

- The table with `ON COMMIT`
```
Table("some_talbe", ..., prefixes=["TEMPORARY"], opengauss_on_commit="PRESERVE ROWS")
```

- The table with `COMPRESS`
```
Table("some_talbe", ..., opengauss_with={"ORIENTATION": "COLUMN"}, opengauss_compress=True)
```

- The table with `TABLESPACE tablespace_name`
```
Table("some_talbe", ..., opengauss_tablespace="tablespace_name")
```

- The table with `PARTITION BY`
```
Table("some_talbe", ..., opengauss_partition_by="RANGE(column_name) "
        "(PARTITION P1 VALUES LESS THAN(10), "
        "PARTITION P2 VALUES LESS THAN(MAXVALUE))")
```

- The table with `ENABLE ROW MOVEMENT`
```
Table("some_talbe", ..., opengauss_partition_by="RANGE(column_name) ...",
    opengauss_enable_row_movement=True)
```

### Vector Data Types
- The vector data type with `select`
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

## Usage of openGauss Features (Centralized)

### Indexing

- Indexing with `CONCURRENTLY`
```
tbl = Table("testtbl", m, Column("data", Integer))
Index("test_idx1", tbl.c.data, opengauss_concurrently=True)
```

## Usage of openGauss Features (Distributed)

### Tables

- The table with `DISTRIBUTE BY`
```
Table("some_table", ..., opengauss_distribute_by="HASH(column_name)")
```
Notes: Tables without distributable columns will be set with `DISTRIBUTE BY REPLICATION`.

- The table with `TO GROUP`
```
Table("some_table", ..., opengauss_to="GROUP group_name")
```


## Release Guide

### Building a Python Wheel Package
```
>>> python -m pip install wheel
>>> python setup.py bdist_wheel
```

### Performing a Local Test

1. Set the environment variables `export LD_LIBRARY_PATH=` and `export PYTHONPATH=` to the directory where the `psycopg2` package is located in the test environment.
2. Install openGauss and modify the database configuration. For details, see "Installing and Configuring the openGauss Commissioning Environment".
3. Run the `tox -e py38` command.


### Installing and Configuring the openGauss Commissioning Environment

1. Add the openGauss OS user `>>> useradd omm -g dbgrp`.
2. Change the user and user group of the openGauss directory `>>> chown omm:dbgrp ${db_dir} -R`.
3. Switch to the new system user `>>> su - omm`.
4. Install openGauss `>>> sh install.sh -w ${db_password} -p 37200`.
5. Start openGauss `>>> gs_ctl start -D ${db_dir}/data/single_node/`.
6. Log in to openGauss `>>> gsql -d postgres -p 37200`.
7. Create a database user, test database, and test schema.
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
8. Modify the openGauss configuration.
```
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "ssl=off"
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "max_prepared_transactions = 100"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "local all scott sha256"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "host all scott 127.0.0.1/32 sha256"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "host all scott 0.0.0.0/0 sha256"
>>> gs_ctl stop -D ${db_dir}/data/single_node/
>>> gs_tl start -D ${db_dir}/data/single_node/
```
9. (Optional) Enable SQL logging.
```
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "log_min_error_statement = error"
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "log_statement = 'all'"
```

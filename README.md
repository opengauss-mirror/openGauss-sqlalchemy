# openGauss-sqlalchemy

适配SQLAlchemy框架的OpenGauss方言库。

项目已通过SQLAlchemy社区用例集。


## 运行依赖

- psycopg2 for opengauss

    从下列地址下载并安装到运行环境中
    > https://gitee.com/opengauss/openGauss-connector-python-psycopg2

    或

    > https://github.com/opengauss-mirror/openGauss-connector-python-psycopg2

### 注意：

使用不匹配的psycopg2会导致SQLAlchemy2.0部分autoescape相关测试用例不通过。

若需要使用autoescape相关功能，请保证环境中正确安装openGauss，并且通过修改变量LD_LIBRARY_PATH保证pg_config来自openGauss。

```
# 配置环境变量
$ export GAUSSHOME=/home/omm/openGauss-server/mppdb_temp_install
$ export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH

$ which pg_config
# 预期结果
'/home/omm/openGauss-server/mppdb_temp_install/bin/pg_config'

# 删除psycopg2 for opengauss
$ pip install --force-reinstall psycopg2

# 若已经安装完成opengauss-sqlalchemy，重新安装
>>> python setup.py install
```

或者安装psycopg2-binary:
```
$ pip uninstall psycopg2
$ pip install psycopg2-binary

# 修改数据库实例下postgresql.conf文件中password_encryption_type = 1
$ vim /datanode/postgresql.conf

# 重启数据库
$ gs_ctl restart -D /datanode

修改密码
$ gsql -d postgres -p <port_num>
openGauss=# alter user <usrname> with password <yourpassword>;
```


## 安装

```
# 从pypi安装
>>> python -m pip install opengauss-sqlalchemy

# 从本地源码安装
>>> python setup.py install
```

## 使用方法

在OpenGauss中创建一个数据库。

```
>>> import sqlalchemy as sa
# 访问集中式模式DB
>>> sa.create_engine('opengauss://username:password@host:port/database_name')
# 或
>>> sa.create_engine('opengauss+psycopg2://username:password@host:port/database_name')
# 访问分布式模式DB
>>> sa.create_engine('opengauss+dc_psycopg2://username:password@host:port/database_name')
# 或
>>> sa.create_engine('opengauss+dc_psycopg2://username:password@/database_name?host=hostA:portA&host=hostB:portB')
```

OpenGauss的数据库开发指南详见 [OpenGauss DeveloperGuide](https://docs.opengauss.org/zh/docs/latest/docs/Developerguide/Developerguide.html)。

## OpenGauss特性的使用方式（集中式和分布式）

### 索引

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

### 表

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

## OpenGauss特性的使用方式（集中式）

### 索引

- Index with `CONCURRENTLY`
```
tbl = Table("testtbl", m, Column("data", Integer))
Index("test_idx1", tbl.c.data, opengauss_concurrently=True)
```

## OpenGauss特性的使用方式（分布式）

### 表

- Table with `DISTRIBUTE BY`
```
Table("some_table", ..., opengauss_distribute_by="HASH(column_name)")
```
NOTE: table without distributable columns will be set with "DISTRIBUTE BY REPLICATION"

- Table with `TO GROUP`
```
Table("some_table", ..., opengauss_to="GROUP group_name")
```


## 发布指南

### 构建 python wheel格式
```
>>> python -m pip install wheel
>>> python setup.py bdist_wheel
```

### 本地测试

1. 设置环境变量 `export LD_LIBRARY_PATH=` 和 `export PYTHONPATH=` 的值为测试环境中 `psycopg2` 包所在的目录.
2. 安装OpenGauss并修改数据库配置, 具体步骤见 "安装并配置OpenGauss调测环境".
3. 执行命令 `tox -e py38`.


### 安装并配置OpenGauss调测环境

1. 添加OpenGauss的操作系统用户 ```>>> useradd omm -g dbgrp```
2. 修改OpenGauss目录的用户和用户组 ```>>> chown omm:dbgrp ${db_dir} -R```
3. 切换到新的系统用户 ```>>> su - omm```
4. 安装OpenGauss ```>>> sh install.sh -w ${db_password} -p 37200```
5. 启动OpenGauss ```>>> gs_ctl start -D ${db_dir}/data/single_node/```
6. 登录OpenGauss ```>>> gsql -d postgres -p 37200```
7. 创建数据库用户、测试数据库和测试模式
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
8. 修改OpenGauss配置
```
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "ssl=off"
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "max_prepared_transactions = 100"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "local all scott sha256"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "host all scott 127.0.0.1/32 sha256"
>>> gs_guc reload -D ${db_dir}/data/single_node/ -h "host all scott 0.0.0.0/0 sha256"
>>> gs_ctl stop -D ${db_dir}/data/single_node/
>>> gs_tl start -D ${db_dir}/data/single_node/
```
9. 启用SQL日志记录（可选）
```
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "log_min_error_statement = error"
>>> gs_guc set -D ${db_dir}/data/single_node/ -c "log_statement = 'all'"
```

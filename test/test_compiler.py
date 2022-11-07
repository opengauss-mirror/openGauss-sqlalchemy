from sqlalchemy import (
    and_, Column, exc, Float, func, Index, Integer, MetaData, Numeric, schema, String, Table, text
)
from sqlalchemy.testing import config
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises_message, AssertsCompiledSQL

from opengauss_sqlalchemy import dc_psycopg2, psycopg2


class DDLCompilerTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = psycopg2.dialect()

    def test_create_partial_index(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))
        idx = Index(
            "test_idx1",
            tbl.c.data,
            opengauss_where=and_(tbl.c.data > 5, tbl.c.data < 10),
        )
        idx = Index(
            "test_idx1",
            tbl.c.data,
            opengauss_where=and_(tbl.c.data > 5, tbl.c.data < 10),
        )

        # test quoting and all that

        idx2 = Index(
            "test_idx2",
            tbl.c.data,
            opengauss_where=and_(tbl.c.data > "a", tbl.c.data < "b's"),
        )
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl (data) "
            "WHERE data > 5 AND data < 10",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data) "
            "WHERE data > 'a' AND data < 'b''s'",
        )

        idx3 = Index(
            "test_idx2",
            tbl.c.data,
            opengauss_where=text("data > 'a' AND data < 'b''s'"),
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx2 ON testtbl (data) "
            "WHERE data > 'a' AND data < 'b''s'",
        )

    def test_create_index_with_ops(self):

        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", String),
            Column("data2", Integer, key="d2"),
        )

        idx = Index(
            "test_idx1",
            tbl.c.data,
            opengauss_ops={"data": "text_pattern_ops"},
        )

        idx2 = Index(
            "test_idx2",
            tbl.c.data,
            tbl.c.d2,
            opengauss_ops={"data": "text_pattern_ops", "d2": "int4_ops"},
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl " "(data text_pattern_ops)",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "(data text_pattern_ops, data2 int4_ops)",
        )

    def test_create_index_with_labeled_ops(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", String),
            Column("data2", Integer, key="d2"),
        )

        idx = Index(
            "test_idx1",
            func.lower(tbl.c.data).label("data_lower"),
            opengauss_ops={"data_lower": "text_pattern_ops"},
        )

        idx2 = Index(
            "test_idx2",
            (func.xyz(tbl.c.data) + tbl.c.d2).label("bar"),
            tbl.c.d2.label("foo"),
            opengauss_ops={"bar": "text_pattern_ops", "foo": "int4_ops"},
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl "
            "(lower(data) text_pattern_ops)",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "((xyz(data) + data2) text_pattern_ops, "
            "data2 int4_ops)",
        )

    def test_create_index_with_text_or_composite(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("d1", String), Column("d2", Integer))

        idx = Index("test_idx1", text("x"))
        tbl.append_constraint(idx)

        idx2 = Index("test_idx2", text("y"), tbl.c.d2)

        idx3 = Index(
            "test_idx2",
            tbl.c.d1,
            text("y"),
            tbl.c.d2,
            opengauss_ops={"d1": "x1", "d2": "x2"},
        )

        idx4 = Index(
            "test_idx2",
            tbl.c.d1,
            tbl.c.d2 > 5,
            text("q"),
            opengauss_ops={"d1": "x1", "d2": "x2"},
        )

        idx5 = Index(
            "test_idx2",
            tbl.c.d1,
            (tbl.c.d2 > 5).label("g"),
            text("q"),
            opengauss_ops={"d1": "x1", "g": "x2"},
        )

        self.assert_compile(
            schema.CreateIndex(idx), "CREATE INDEX test_idx1 ON testtbl (x)"
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (y, d2)",
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx2 ON testtbl (d1 x1, y, d2 x2)",
        )

        # note that at the moment we do not expect the 'd2' op to
        # pick up on the "d2 > 5" expression
        self.assert_compile(
            schema.CreateIndex(idx4),
            "CREATE INDEX test_idx2 ON testtbl (d1 x1, (d2 > 5), q)",
        )

        # however it does work if we label!
        self.assert_compile(
            schema.CreateIndex(idx5),
            "CREATE INDEX test_idx2 ON testtbl (d1 x1, (d2 > 5) x2, q)",
        )

    def test_create_index_with_using(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        idx1 = Index("test_idx1", tbl.c.data)
        idx2 = Index("test_idx2", tbl.c.data, opengauss_using="btree")
        idx3 = Index("test_idx3", tbl.c.data, opengauss_using="hash")

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl " "(data)",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl " "USING btree (data)",
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl " "USING hash (data)",
        )

    def test_create_index_with_local_no_partition(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", Integer),
            opengauss_partition_by="RANGE (data)"
        )
        idx1 = Index("test_idx1", tbl.c.data)
        idx2 = Index("test_idx2", tbl.c.data, opengauss_local=[""])

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data)",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data) LOCAL",
        )

    def test_create_index_with_local_partitions(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", Integer),
            opengauss_partition_by="RANGE (data)"
        )
        idx1 = Index(
            "test_idx1",
            tbl.c.data,
            opengauss_local=[
                "PARTITION data_index1",
                "PARTITION data_index2 TABLESPACE example3",
            ]
        )
        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data) LOCAL "
            "(PARTITION data_index1, PARTITION data_index2 TABLESPACE example3)",
        )

    def test_create_index_with_with(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        idx1 = Index("test_idx1", tbl.c.data)
        idx2 = Index(
            "test_idx2", tbl.c.data, opengauss_with={"fillfactor": 50}
        )
        idx3 = Index(
            "test_idx3",
            tbl.c.data,
            opengauss_using="gist",
            opengauss_with={"buffering": "off"},
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl " "(data)",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "(data) "
            "WITH (fillfactor = 50)",
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl "
            "USING gist (data) "
            "WITH (buffering = off)",
        )

    def test_create_index_with_using_unusual_conditions(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        self.assert_compile(
            schema.CreateIndex(
                Index("test_idx1", tbl.c.data, opengauss_using="GIST")
            ),
            "CREATE INDEX test_idx1 ON testtbl " "USING gist (data)",
        )

        self.assert_compile(
            schema.CreateIndex(
                Index(
                    "test_idx1",
                    tbl.c.data,
                    opengauss_using="some_custom_method",
                )
            ),
            "CREATE INDEX test_idx1 ON testtbl "
            "USING some_custom_method (data)",
        )

        assert_raises_message(
            exc.CompileError,
            "Unexpected SQL phrase: 'gin invalid sql'",
            schema.CreateIndex(
                Index(
                    "test_idx2", tbl.c.data, opengauss_using="gin invalid sql"
                )
            ).compile,
            dialect=psycopg2.dialect(),
        )

    def test_create_index_with_tablespace(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        idx1 = Index("test_idx1", tbl.c.data)
        idx2 = Index(
            "test_idx2", tbl.c.data, opengauss_tablespace="sometablespace"
        )
        idx3 = Index(
            "test_idx3",
            tbl.c.data,
            opengauss_tablespace="another table space",
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl " "(data)",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "(data) "
            "TABLESPACE sometablespace",
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl "
            "(data) "
            'TABLESPACE "another table space"',
        )

    def test_create_index_with_multiple_options(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        idx1 = Index(
            "test_idx1",
            tbl.c.data,
            opengauss_using="btree",
            opengauss_tablespace="atablespace",
            opengauss_with={"fillfactor": 60},
            opengauss_where=and_(tbl.c.data > 5, tbl.c.data < 10),
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl "
            "USING btree (data) "
            "WITH (fillfactor = 60) "
            "TABLESPACE atablespace "
            "WHERE data > 5 AND data < 10",
        )

    def test_create_index_expr_gets_parens(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("x", Integer), Column("y", Integer))

        idx1 = Index("test_idx1", 5 / (tbl.c.x + tbl.c.y))
        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl ((5 / (x + y)))",
        )

    def test_create_index_literals(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))

        idx1 = Index("test_idx1", tbl.c.data + 5)
        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl ((data + 5))",
        )

    def test_create_index_concurrently(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))

        idx1 = Index("test_idx1", tbl.c.data, opengauss_concurrently=True)
        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX CONCURRENTLY test_idx1 ON testtbl (data)",
        )

    def test_drop_index_concurrently(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))

        idx1 = Index("test_idx1", tbl.c.data, opengauss_concurrently=True)
        self.assert_compile(
            schema.DropIndex(idx1), "DROP INDEX CONCURRENTLY test_idx1"
        )

    def test_create_table_with_with_clause(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            opengauss_with={"ORIENTATION": "COLUMN"},
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) WITH (ORIENTATION = COLUMN)",
        )

    def test_create_table_with_tablespace(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            opengauss_tablespace="sometablespace",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) TABLESPACE sometablespace",
        )

    def test_create_table_with_tablespace_quoted(self):
        # testing quoting of tablespace name
        m = MetaData()
        tbl = Table(
            "anothertable",
            m,
            Column("id", Integer),
            opengauss_tablespace="table",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            'CREATE TABLE anothertable (id INTEGER) TABLESPACE "table"',
        )

    def test_create_table_with_oncommit_option(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            prefixes=["TEMPORARY"],
            opengauss_on_commit="DROP",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TEMPORARY TABLE atable (id INTEGER) ON COMMIT DROP",
        )

    def test_create_table_with_compress(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            opengauss_with={"ORIENTATION": "COLUMN"},
            opengauss_compress=True,
        )
        self.assert_compile(
            schema.CreateTable(tbl),
             "CREATE TABLE atable (id INTEGER) WITH (ORIENTATION = COLUMN) COMPRESS",
        )

    def test_create_table_partition_by_list(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            Column("part_column", Integer),
            opengauss_partition_by="LIST (part_column)",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, part_column INTEGER) "
            "PARTITION BY LIST (part_column)",
        )

    def test_create_table_partition_by_range(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            Column("part_column", Integer),
            opengauss_partition_by="RANGE (part_column)",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, part_column INTEGER) "
            "PARTITION BY RANGE (part_column)",
        )

    def test_create_table_enable_row_movement(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            Column("part_column", Integer),
            opengauss_partition_by="RANGE (part_column)",
            opengauss_enable_row_movement=True,
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, part_column INTEGER) "
            "PARTITION BY RANGE (part_column) "
            "ENABLE ROW MOVEMENT",
        )


class DDLCompilerTest_dc_psycopg2(DDLCompilerTest):
    __dialect__ = dc_psycopg2.dialect()

    def test_create_index_concurrently(self):
        config.skip_test("Distribited mode unsupport create index concurrently")

    def test_drop_index_concurrently(self):
        config.skip_test("Distribited mode unsupport drop index concurrently")

    def test_create_table_distribute_by_replication(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            opengauss_distribute_by="REPLICATION",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) "
            "DISTRIBUTE BY REPLICATION",
        )

    def test_create_table_distribute_by_hash(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            opengauss_distribute_by="HASH(id)",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) "
            "DISTRIBUTE BY HASH(id)",
        )

    def test_create_table_distribute_by_range(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            Column("distri_col", Integer),
            opengauss_distribute_by="RANGE(distri_col) (SLICE s1 VALUES LESS THAN(10), "
                                    "SLICE s2 VALUES LESS THAN (MAXVALUE))",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, distri_col INTEGER) "
            "DISTRIBUTE BY RANGE(distri_col) (SLICE s1 VALUES LESS THAN(10), SLICE s2 VALUES LESS THAN (MAXVALUE))",
        )

    def test_create_table_distribute_by_list(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            Column("distri_col", String(16)),
            opengauss_distribute_by="LIST(distri_col) (SLICE s1 VALUES ('D1'), "
                                    "SLICE s2 VALUES (DEFAULT))",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, distri_col VARCHAR(16)) "
            "DISTRIBUTE BY LIST(distri_col) (SLICE s1 VALUES ('D1'), SLICE s2 VALUES (DEFAULT))",
        )

    def test_create_table_without_distributable_column_float(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("undistributable_col", Float),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (undistributable_col FLOAT) "
            "DISTRIBUTE BY REPLICATION",
        )

    def test_create_table_without_distributable_column_float_as_decimal(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("undistributable_col", Float(precision=8, asdecimal=True)),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (undistributable_col FLOAT(8)) "
            "DISTRIBUTE BY REPLICATION",
        )

    def test_create_table_with_distributable_column_numeric(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("distributable_col", Numeric(18, 14)),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (distributable_col NUMERIC(18, 14))",
        )

    def test_create_table_with_distributable_column_varchar(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("distributable_col", String(40)),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (distributable_col VARCHAR(40))",
        )

    def test_create_table_with_to_group(self):
        m = MetaData()
        tbl = Table(
            "atable", m, Column("id", Integer), opengauss_to="GROUP group_s1",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) TO GROUP group_s1",
        )

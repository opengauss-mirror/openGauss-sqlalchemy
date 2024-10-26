# -*- coding: utf-8 -*-
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# Copyright (C) 2021-2022 Huawei Technologies Co.,Ltd.
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

import operator

import sqlalchemy as sa
from sqlalchemy.schema import DDL, Index
from sqlalchemy import inspect, testing
from sqlalchemy import types as sql_types
from sqlalchemy.testing.provision import get_temp_table_name, temp_table_keyword_args
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import ComponentReflectionTestExtra as _ComponentReflectionTestExtra
from sqlalchemy.testing.suite.test_cte import CTETest as _CTETest
from sqlalchemy.testing.suite.test_ddl import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite.test_reflection import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite.test_reflection import CompositeKeyReflectionTest as _CompositeKeyReflectionTest
from sqlalchemy.testing.suite.test_reflection import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite.test_results import ServerSideCursorsTest as _ServerSideCursorsTest
from sqlalchemy.testing.suite.test_select import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite.test_select import JoinTest as _JoinTest
from sqlalchemy.testing.suite.test_unicode_ddl import UnicodeSchemaTest as _UnicodeSchemaTest
from sqlalchemy.engine import ObjectScope, ObjectKind, Inspector
from sqlalchemy.testing import mock, assert_raises_message
from sqlalchemy.testing.suite.test_dialect import IsolationLevelTest as _IsolationLevelTest
from sqlalchemy.testing.suite.test_types import TrueDivTest as _TrueDivTest

class CTETest(_CTETest):
    @classmethod
    def define_tables(cls, metadata):
        if testing.requires.foreign_keys.enabled:
            Table(
                "some_table",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("data", String(50)),
                Column("parent_id", ForeignKey("some_table.id")),
            )
        else:
            Table(
                "some_table",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("data", String(50)),
                Column("parent_id", Integer),
            )

        Table(
            "some_other_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            Column("parent_id", Integer),
        )


class LongNameBlowoutTest(_LongNameBlowoutTest):
    @testing.combinations(
        ("fk", testing.requires.foreign_keys),
        ("pk",),
        ("ix",),
        ("ck", testing.requires.check_constraint_reflection.as_skips()),
        ("uq", testing.requires.unique_constraint_reflection.as_skips()),
        argnames="type_",
    )
    def test_long_convention_name(self, type_, metadata, connection):
        actual_name, reflected_name = getattr(self, type_)(
            metadata, connection
        )

        assert len(actual_name) > 255

        if reflected_name is not None:
            overlap = actual_name[0: len(reflected_name)]
            if len(overlap) < len(actual_name):
                eq_(overlap[0:-5], reflected_name[0: len(overlap) - 5])
            else:
                eq_(overlap, reflected_name)

def _multi_combination(fn):
    schema = testing.combinations(
        None,
        (
            lambda: config.test_schema,
            testing.requires.schemas,
        ),
        argnames="schema",
    )
    scope = testing.combinations(
        ObjectScope.DEFAULT,
        ObjectScope.TEMPORARY,
        ObjectScope.ANY,
        argnames="scope",
    )
    kind = testing.combinations(
        ObjectKind.TABLE,
        ObjectKind.VIEW,
        ObjectKind.MATERIALIZED_VIEW,
        ObjectKind.ANY,
        ObjectKind.ANY_VIEW,
        ObjectKind.TABLE | ObjectKind.VIEW,
        ObjectKind.TABLE | ObjectKind.MATERIALIZED_VIEW,
        argnames="kind",
    )
    filter_names = testing.combinations(True, False, argnames="use_filter")

    return schema(scope(kind(filter_names(fn))))

class ComponentReflectionTest(_ComponentReflectionTest):
    @classmethod
    def define_reflected_tables(cls, metadata, schema):
        if schema:
            schema_prefix = schema + "."
        else:
            schema_prefix = ""

        if testing.requires.self_referential_foreign_keys.enabled:
            parent_id_args = (
                ForeignKey(
                    "%susers.user_id" % schema_prefix, name="user_id_fk"
                ),
            )
        else:
            parent_id_args = ()

        users = Table(
            "users",
            metadata,
            Column("user_id", sa.INT, primary_key=True),
            Column("test1", sa.CHAR(5), nullable=False),
            Column("test2", sa.Float(), nullable=False),
            Column("parent_user_id", sa.Integer, *parent_id_args),
            sa.CheckConstraint(
                "test2 > 0",
                name="zz_test2_gt_zero",
                comment="users check constraint",
            ),
            sa.CheckConstraint("test2 <= 1000"),
            schema=schema,
            test_needs_fk=True,
        )

        if testing.requires.foreign_keys.enabled:
            # distributed opengauss does NOT support foreign keys
            Table(
                "dingalings",
                metadata,
                Column("dingaling_id", sa.Integer, primary_key=True),
                Column(
                    "address_id",
                    sa.Integer,
                    ForeignKey(
                        "%semail_addresses.address_id" % schema_prefix,
                        name="zz_email_add_id_fg",
                        comment="di fk comment",
                    ),
                ),
                Column(
                    "id_user",
                    sa.Integer,
                    ForeignKey("%susers.user_id" % schema_prefix),
                ),
                Column("data", sa.String(30), unique=True),
                sa.CheckConstraint(
                    "address_id > 0 AND address_id < 1000",
                    name="address_id_gt_zero",
                ),
                sa.UniqueConstraint(
                    "address_id",
                    "dingaling_id",
                    name="zz_dingalings_multiple",
                    comment="di unique comment",
                ),
                schema=schema,
                test_needs_fk=True,
            )
            Table(
                "email_addresses",
                metadata,
                Column("address_id", sa.Integer),
                Column("remote_user_id", sa.Integer, ForeignKey(users.c.user_id)),
                Column("email_address", sa.String(20), index=True),
                sa.PrimaryKeyConstraint(
                    "address_id", name="email_ad_pk", comment="ea pk comment"
                ),
                schema=schema,
                test_needs_fk=True,
            )
        else:
            Table(
                "dingalings",
                metadata,
                Column("dingaling_id", sa.Integer, primary_key=True),
                Column(
                    "address_id",
                    sa.Integer,
                ),
                Column(
                    "id_user",
                    sa.Integer,
                ),
                Column("data", sa.String(30), unique=True),
                sa.CheckConstraint(
                    "address_id > 0 AND address_id < 1000",
                    name="address_id_gt_zero",
                ),
                sa.UniqueConstraint(
                    "address_id",
                    "dingaling_id",
                    name="zz_dingalings_multiple",
                    comment="di unique comment",
                ),
                schema=schema,
                test_needs_fk=True,
            )
            Table(
                "email_addresses",
                metadata,
                Column("address_id", sa.Integer),
                Column("remote_user_id", sa.Integer),
                Column("email_address", sa.String(20), index=True),
                sa.PrimaryKeyConstraint(
                    "address_id", name="email_ad_pk", comment="ea pk comment"
                ),
                schema=schema,
                test_needs_fk=True,
            )
        Table(
            "comment_test",
            metadata,
            Column("id", sa.Integer, primary_key=True, comment="id comment"),
            Column("data", sa.String(20), comment="data % comment"),
            Column(
                "d2",
                sa.String(20),
                comment=r"""Comment types type speedily ' " \ '' Fun!""",
            ),
            Column("d3", sa.String(42), comment="Comment\nwith\rescapes"),
            schema=schema,
            comment=r"""the test % ' " \ table comment""",
        )
        Table(
            "no_constraints",
            metadata,
            Column("data", sa.String(20)),
            schema=schema,
            comment="no\nconstraints\rhas\fescaped\vcomment",
        )

        if testing.requires.cross_schema_fk_reflection.enabled:
            if schema is None:
                Table(
                    "local_table",
                    metadata,
                    Column("id", sa.Integer, primary_key=True),
                    Column("data", sa.String(20)),
                    Column(
                        "remote_id",
                        ForeignKey(
                            "%s.remote_table_2.id" % testing.config.test_schema
                        ),
                    ),
                    test_needs_fk=True,
                    schema=config.db.dialect.default_schema_name,
                )
            else:
                Table(
                    "remote_table",
                    metadata,
                    Column("id", sa.Integer, primary_key=True),
                    Column(
                        "local_id",
                        ForeignKey(
                            "%s.local_table.id"
                            % config.db.dialect.default_schema_name
                        ),
                    ),
                    Column("data", sa.String(20)),
                    schema=schema,
                    test_needs_fk=True,
                )
                Table(
                    "remote_table_2",
                    metadata,
                    Column("id", sa.Integer, primary_key=True),
                    Column("data", sa.String(20)),
                    schema=schema,
                    test_needs_fk=True,
                )

        if testing.requires.index_reflection.enabled:
            Index("users_t_idx", users.c.test1, users.c.test2, unique=True)
            Index(
                "users_all_idx", users.c.user_id, users.c.test2, users.c.test1
            )

            if not schema:
                # test_needs_fk is at the moment to force MySQL InnoDB
                noncol_idx_test_nopk = Table(
                    "noncol_idx_test_nopk",
                    metadata,
                    Column("q", sa.String(5)),
                    test_needs_fk=True,
                )

                noncol_idx_test_pk = Table(
                    "noncol_idx_test_pk",
                    metadata,
                    Column("id", sa.Integer, primary_key=True),
                    Column("q", sa.String(5)),
                    test_needs_fk=True,
                )

                if (
                    testing.requires.indexes_with_ascdesc.enabled
                    and testing.requires.reflect_indexes_with_ascdesc.enabled
                ):
                    Index("noncol_idx_nopk", noncol_idx_test_nopk.c.q.desc())
                    Index("noncol_idx_pk", noncol_idx_test_pk.c.q.desc())

        if testing.requires.view_column_reflection.enabled:
            cls.define_views(metadata, schema)

        if not schema and testing.requires.temp_table_reflection.enabled:
            cls.define_temp_tables(metadata)

    @classmethod
    def define_temp_tables(cls, metadata):
        kw = temp_table_keyword_args(config, config.db)
        table_name = cls.temp_table_name()
        user_tmp = Table(
            table_name,
            metadata,
            # local temp table does NOT support serial column in opengauss
            Column("id", sa.INT),
            Column("name", sa.VARCHAR(50)),
            Column("foo", sa.INT),
            # disambiguate temp table unique constraint names.  this is
            # pretty arbitrary for a generic dialect however we are doing
            # it to suit SQL Server which will produce name conflicts for
            # unique constraints created against temp tables in different
            # databases.
            # https://www.arbinada.com/en/node/1645
            sa.UniqueConstraint("name", name=f"user_tmp_uq_{config.ident}"),
            sa.Index("user_tmp_ix", "foo"),
            **kw
        )
        if (
            testing.requires.view_reflection.enabled
            and testing.requires.temporary_views.enabled
        ):
            event.listen(
                user_tmp,
                "after_create",
                DDL(
                    "create temporary view user_tmp_v as "
                    "select * from user_tmp_%s" % config.ident
                ),
            )
            event.listen(user_tmp, "before_drop", DDL("drop view user_tmp_v"))

    @testing.combinations(
        (True, testing.requires.schemas), (False,), argnames="use_schema"
    )
    @testing.requires.unique_constraint_reflection
    def test_get_unique_constraints(self, metadata, connection, use_schema):
        if use_schema:
            schema = config.test_schema
        else:
            schema = None
        uniques = sorted(
            [
                {"name": "unique_a", "column_names": ["a"]},
                {"name": "unique_a_b_c", "column_names": ["a", "b", "c"]},
                {"name": "unique_c_a_b", "column_names": ["c", "a", "b"]},
                {"name": "unique_asc_key", "column_names": ["asc", "key"]},
                {"name": "i.have.dots", "column_names": ["b"]},
                {"name": "i have spaces", "column_names": ["c"]},
            ],
            key=operator.itemgetter("name"),
        )

        if testing.against("opengauss+dc_psycopg2"):
            table = Table(
                "testtbl",
                metadata,
                Column("a", sa.String(20)),
                Column("b", sa.String(30)),
                Column("c", sa.Integer),
                # reserved identifiers
                Column("asc", sa.String(30)),
                Column("key", sa.String(30)),
                schema=schema,
                opengauss_distribute_by='REPLICATION',
            )
        else:
            table = Table(
                "testtbl",
                metadata,
                Column("a", sa.String(20)),
                Column("b", sa.String(30)),
                Column("c", sa.Integer),
                # reserved identifiers
                Column("asc", sa.String(30)),
                Column("key", sa.String(30)),
                schema=schema,
            )
        for uc in uniques:
            table.append_constraint(
                sa.UniqueConstraint(*uc["column_names"], name=uc["name"])
            )
        table.create(connection)

        inspector = inspect(connection)
        reflected = sorted(
            inspector.get_unique_constraints("testtbl", schema=schema),
            key=operator.itemgetter("name"),
        )

        names_that_duplicate_index = set()

        for orig, refl in zip(uniques, reflected):
            # Different dialects handle duplicate index and constraints
            # differently, so ignore this flag
            dupe = refl.pop("duplicates_index", None)
            if dupe:
                names_that_duplicate_index.add(dupe)
            eq_(refl.pop("comment", None), None)
            eq_(orig, refl)

        reflected_metadata = MetaData()
        reflected = Table(
            "testtbl",
            reflected_metadata,
            autoload_with=connection,
            schema=schema,
        )

        # test "deduplicates for index" logic.   MySQL and Oracle
        # "unique constraints" are actually unique indexes (with possible
        # exception of a unique that is a dupe of another one in the case
        # of Oracle).  make sure # they aren't duplicated.
        idx_names = {idx.name for idx in reflected.indexes}
        uq_names = {
            uq.name
            for uq in reflected.constraints
            if isinstance(uq, sa.UniqueConstraint)
        }.difference(["unique_c_a_b"])

        assert not idx_names.intersection(uq_names)
        if names_that_duplicate_index:
            eq_(names_that_duplicate_index, idx_names)
            eq_(uq_names, set())

        no_cst = self.tables.no_constraints.name
        eq_(inspector.get_unique_constraints(no_cst, schema=schema), [])

    def exp_columns(
        self,
        schema=None,
        scope=ObjectScope.ANY,
        kind=ObjectKind.ANY,
        filter_names=None,
    ):
        def col(
            name, auto=False, default=mock.ANY, comment=None, nullable=True
        ):
            res = {
                "name": name,
                "autoincrement": auto,
                "type": mock.ANY,
                "default": default,
                "comment": comment,
                "nullable": nullable,
            }
            if auto == "omit":
                res.pop("autoincrement")
            return res

        def pk(name, **kw):
            kw = {"auto": True, "default": mock.ANY, "nullable": False, **kw}
            return col(name, **kw)

        materialized = {
            (schema, "dingalings_v"): [
                col("dingaling_id", auto="omit", nullable=mock.ANY),
                col("address_id"),
                col("id_user"),
                col("data"),
            ]
        }
        views = {
            (schema, "email_addresses_v"): [
                col("address_id", auto="omit", nullable=mock.ANY),
                col("remote_user_id"),
                col("email_address"),
            ],
            (schema, "users_v"): [
                col("user_id", auto="omit", nullable=mock.ANY),
                col("test1", nullable=mock.ANY),
                col("test2", nullable=mock.ANY),
                col("parent_user_id"),
            ],
            (schema, "user_tmp_v"): [
                col("id", auto="omit", nullable=mock.ANY),
                col("name"),
                col("foo"),
            ],
        }
        self._resolve_views(views, materialized)
        tables = {
            (schema, "users"): [
                pk("user_id"),
                col("test1", nullable=False),
                col("test2", nullable=False),
                col("parent_user_id"),
            ],
            (schema, "dingalings"): [
                pk("dingaling_id"),
                col("address_id"),
                col("id_user"),
                col("data"),
            ],
            (schema, "email_addresses"): [
                pk("address_id"),
                col("remote_user_id"),
                col("email_address"),
            ],
            (schema, "comment_test"): [
                pk("id", comment="id comment"),
                col("data", comment="data % comment"),
                col(
                    "d2",
                    comment=r"""Comment types type speedily ' " \ '' Fun!""",
                ),
                col("d3", comment="Comment\nwith\rescapes"),
            ],
            (schema, "no_constraints"): [col("data")],
            (schema, "local_table"): [pk("id"), col("data"), col("remote_id")],
            (schema, "remote_table"): [pk("id"), col("local_id"), col("data")],
            (schema, "remote_table_2"): [pk("id"), col("data")],
            (schema, "noncol_idx_test_nopk"): [col("q")],
            (schema, "noncol_idx_test_pk"): [pk("id"), col("q")],
            (schema, self.temp_table_name()): [
                pk("id",  nullable=True, auto=False),
                col("name",  nullable=True, auto=False),
                col("foo",  nullable=True, auto=False),
            ],
        }
        res = self._resolve_kind(kind, tables, views, materialized)
        res = self._resolve_names(schema, scope, filter_names, res)
        return res

    @property
    def _required_column_keys(self):
        return {"name", "type", "nullable", "default"}

    def exp_pks(
            self,
            schema=None,
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            filter_names=None,
    ):
        def pk(*cols, name=mock.ANY, comment=None):
            return {
                "constrained_columns": list(cols),
                "name": name,
                "comment": comment,
            }

        empty = pk(name=None)
        if testing.requires.materialized_views_reflect_pk.enabled:
            materialized = {(schema, "dingalings_v"): pk("dingaling_id")}
        else:
            materialized = {(schema, "dingalings_v"): empty}
        views = {
            (schema, "email_addresses_v"): empty,
            (schema, "users_v"): empty,
            (schema, "user_tmp_v"): empty,
        }
        self._resolve_views(views, materialized)
        tables = {
            (schema, "users"): pk("user_id"),
            (schema, "dingalings"): pk("dingaling_id"),
            (schema, "email_addresses"): pk(
                "address_id", name="email_ad_pk", comment="ea pk comment"
            ),
            (schema, "comment_test"): pk("id"),
            (schema, "no_constraints"): empty,
            (schema, "local_table"): pk("id"),
            (schema, "remote_table"): pk("id"),
            (schema, "remote_table_2"): pk("id"),
            (schema, "noncol_idx_test_nopk"): empty,
            (schema, "noncol_idx_test_pk"): pk("id"),
            (schema, self.temp_table_name()): pk(name=None),
        }
        if not testing.requires.reflects_pk_names.enabled:
            for val in tables.values():
                if val["name"] is not None:
                    val["name"] = mock.ANY
        res = self._resolve_kind(kind, tables, views, materialized)
        res = self._resolve_names(schema, scope, filter_names, res)
        return res


class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):
    @classmethod
    def define_tables(cls, metadata):
        tb1 = Table(
            "tb1",
            metadata,
            Column("id", Integer),
            Column("attr", Integer),
            Column("name", sql_types.VARCHAR(20)),
            sa.PrimaryKeyConstraint("name", "id", "attr", name="pk_tb1"),
            schema=None,
            test_needs_fk=True,
        )
        if testing.requires.foreign_key_constraint_reflection.enabled:
            Table(
                "tb2",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("pid", Integer),
                Column("pattr", Integer),
                Column("pname", sql_types.VARCHAR(20)),
                sa.ForeignKeyConstraint(
                    ["pname", "pid", "pattr"],
                    [tb1.c.name, tb1.c.id, tb1.c.attr],
                    name="fk_tb1_name_id_attr",
                ),
                schema=None,
                test_needs_fk=True,
            )


class ComponentReflectionTestExtra(_ComponentReflectionTestExtra):
    @testing.combinations(
        (
            None,
            "CASCADE",
            None,
            testing.requires.foreign_key_constraint_option_reflection_ondelete,
        ),
        (
            None,
            None,
            "SET NULL",
            testing.requires.foreign_key_constraint_option_reflection_onupdate,
        ),
        (
            {},
            None,
            "NO ACTION",
            testing.requires.foreign_key_constraint_option_reflection_onupdate,
        ),
        (
            {},
            "NO ACTION",
            None,
            testing.requires.fk_constraint_option_reflection_ondelete_noaction,
        ),
        (
            None,
            None,
            "RESTRICT",
            testing.requires.fk_constraint_option_reflection_onupdate_restrict,
        ),
        (
            None,
            "RESTRICT",
            None,
            testing.requires.fk_constraint_option_reflection_ondelete_restrict,
        ),
        argnames="expected,ondelete,onupdate",
    )
    def test_get_foreign_key_options(
        self, connection, metadata, expected, ondelete, onupdate
    ):
        options = {}
        if ondelete:
            options["ondelete"] = ondelete
        if onupdate:
            options["onupdate"] = onupdate

        if expected is None:
            expected = options

        Table(
            "x",
            metadata,
            Column("id", Integer, primary_key=True),
            test_needs_fk=True,
        )

        Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x_id", Integer, sa.ForeignKey("x.id", name="xid")),
            Column("test", String(10)),
            test_needs_fk=True,
        )

        # tid is system column name in opengauss, change it to "tid_"
        Table(
            "user",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50), nullable=False),
            Column("tid_", Integer),
            sa.ForeignKeyConstraint(
                ["tid_"], ["table.id"], name="myfk", **options
            ),
            test_needs_fk=True,
        )

        metadata.create_all(connection)

        insp = inspect(connection)

        # test 'options' is always present for a backend
        # that can reflect these, since alembic looks for this
        opts = insp.get_foreign_keys("table")[0]["options"]

        eq_({k: opts[k] for k in opts if opts[k]}, {})

        opts = insp.get_foreign_keys("user")[0]["options"]
        eq_(opts, expected)

    @testing.requires.indexes_with_expressions
    def test_reflect_expression_based_indexes(self, metadata, connection):
        t = Table(
            "t",
            metadata,
            Column("x", String(30)),
            Column("y", String(30)),
            Column("z", String(30)),
        )

        Index("t_idx", func.lower(t.c.x), t.c.z, func.lower(t.c.y))
        long_str = "long string " * 100
        Index("t_idx_long", func.coalesce(t.c.x, long_str))
        Index("t_idx_2", t.c.x)

        metadata.create_all(connection)

        insp = inspect(connection)

        expected = [
            {
                "name": "t_idx_2",
                "unique": False,
                "column_names": ["x"],
            }
        ]

        class lower_index_str(str):
            def __eq__(self, other):
                ol = other.lower()
                # test that lower and x or y are in the string
                return "lower" in ol and ("x" in ol or "y" in ol)

        class coalesce_index_str(str):
            def __eq__(self, other):
                # test that coalesce and the string is in other
                return "coalesce" in other.lower() and long_str in other

        if testing.requires.reflect_indexes_with_expressions.enabled:
            expr_index = {
                "name": "t_idx",
                "unique": False,
                "column_names": [None, "z", None],
                "expressions": [
                    lower_index_str("lower(x)"),
                    "z",
                    lower_index_str("lower(y)"),
                ],
            }
            expected.insert(0, expr_index)

            expr_index_long = {
                "name": "t_idx_long",
                "unique": False,
                "column_names": [None],
                "expressions": [
                    coalesce_index_str(f"coalesce(x, '{long_str}' ::character varying))")
                ],
            }
            expected.append(expr_index_long)

            eq_(insp.get_indexes("t"), expected)
            m2 = MetaData()
            t2 = Table("t", m2, autoload_with=connection)
        else:
            eq_(insp.get_indexes("t"), expected)
            m2 = MetaData()
            t2 = Table("t", m2, autoload_with=connection)

        self.compare_table_index_with_expected(
            t2, expected, connection.engine.name
        )


class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    @classmethod
    def define_tables(cls, metadata):
        if testing.requires.foreign_keys.enabled:
            Table(
                "quote ' one",
                metadata,
                Column("id", Integer),
                Column("name", String(50)),
                Column("data", String(50)),
                Column("related_id", Integer),
                sa.PrimaryKeyConstraint("id", name="pk quote ' one"),
                sa.Index("ix quote ' one", "name"),
                sa.UniqueConstraint(
                    "data",
                    name="uq quote' one",
                ),
                sa.ForeignKeyConstraint(
                    ["id"], ["related.id"], name="fk quote ' one"
                ),
                sa.CheckConstraint("name != 'foo'", name="ck quote ' one"),
                comment=r"""quote ' one comment""",
                test_needs_fk=True,
            )

            if testing.requires.symbol_names_w_double_quote.enabled:
                Table(
                    'quote " two',
                    metadata,
                    Column("id", Integer),
                    Column("name", String(50)),
                    Column("data", String(50)),
                    Column("related_id", Integer),
                    sa.PrimaryKeyConstraint("id", name='pk quote " two'),
                    sa.Index('ix quote " two', "name"),
                    sa.UniqueConstraint(
                        "data",
                        name='uq quote" two',
                    ),
                    sa.ForeignKeyConstraint(
                        ["id"], ["related.id"], name='fk quote " two'
                    ),
                    sa.CheckConstraint("name != 'foo'", name='ck quote " two '),
                    comment=r"""quote " two comment""",
                    test_needs_fk=True,
                )
        else:
            Table(
                "quote ' one",
                metadata,
                Column("id", Integer),
                Column("name", String(50)),
                Column("data", String(50)),
                Column("related_id", Integer),
                sa.PrimaryKeyConstraint("id", name="pk quote ' one"),
                sa.Index("ix quote ' one", "name"),
                sa.UniqueConstraint(
                    "data",
                    name="uq quote' one",
                ),
                sa.CheckConstraint("name != 'foo'", name="ck quote ' one"),
                comment=r"""quote ' one comment""",
                test_needs_fk=True,
                opengauss_distribute_by='REPLICATION',
            )

            if testing.requires.symbol_names_w_double_quote.enabled:
                Table(
                    'quote " two',
                    metadata,
                    Column("id", Integer),
                    Column("name", String(50)),
                    Column("data", String(50)),
                    Column("related_id", Integer),
                    sa.PrimaryKeyConstraint("id", name='pk quote " two'),
                    sa.Index('ix quote " two', "name"),
                    sa.UniqueConstraint(
                        "data",
                        name='uq quote" two',
                    ),
                    sa.CheckConstraint("name != 'foo'", name='ck quote " two '),
                    comment=r"""quote " two comment""",
                    test_needs_fk=True,
                    opengauss_distribute_by='REPLICATION',
                )

        Table(
            "related",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("related", Integer),
            test_needs_fk=True,
        )

        if testing.requires.view_column_reflection.enabled:

            if testing.requires.symbol_names_w_double_quote.enabled:
                names = [
                    "quote ' one",
                    'quote " two',
                ]
            else:
                names = [
                    "quote ' one",
                ]
            for name in names:
                query = "CREATE VIEW %s AS SELECT * FROM %s" % (
                    config.db.dialect.identifier_preparer.quote(
                        "view %s" % name
                    ),
                    config.db.dialect.identifier_preparer.quote(name),
                )

                event.listen(metadata, "after_create", DDL(query))
                event.listen(
                    metadata,
                    "before_drop",
                    DDL(
                        "DROP VIEW %s"
                        % config.db.dialect.identifier_preparer.quote(
                            "view %s" % name
                        )
                    ),
                )

    def quote_fixtures(fn):
        return testing.combinations(
            ("quote ' one",),
            ('quote " two', testing.requires.symbol_names_w_double_quote),
        )(fn)

    @quote_fixtures
    @testing.requires.foreign_keys
    def test_get_foreign_keys(self, name):
        insp = inspect(config.db)
        assert insp.get_foreign_keys(name)


class ServerSideCursorsTest(_ServerSideCursorsTest):
    def _is_server_side(self, cursor):
        # TODO: this is a huge issue as it prevents these tests from being
        # usable by third party dialects.
        if self.engine.dialect.driver in ("psycopg2", "dc_psycopg2"):
            return bool(cursor.name)
        elif self.engine.dialect.driver == "pymysql":
            sscursor = __import__("pymysql.cursors").cursors.SSCursor
            return isinstance(cursor, sscursor)
        elif self.engine.dialect.driver in ("aiomysql", "asyncmy"):
            return cursor.server_side
        elif self.engine.dialect.driver == "mysqldb":
            sscursor = __import__("MySQLdb.cursors").cursors.SSCursor
            return isinstance(cursor, sscursor)
        elif self.engine.dialect.driver == "mariadbconnector":
            return not cursor.buffered
        elif self.engine.dialect.driver in ("asyncpg", "aiosqlite"):
            return cursor.server_side
        elif self.engine.dialect.driver == "pg8000":
            return getattr(cursor, "server_side", False)
        else:
            return False


class FetchLimitOffsetTest(_FetchLimitOffsetTest):
    def test_limit_render_multiple_times(self, connection):
        table = self.tables.some_table

        if testing.requires.limit_with_strict_isotonicity.enabled:
            stmt = select(table.c.id).limit(1).scalar_subquery()
        else:
            # LIMIT in distributed opengauss does not have strict isotonicity,
            # so use it with `order_by` if you need strict isotonicity.
            stmt = select(table.c.id).order_by(table.c.id).limit(1).scalar_subquery()

        u = union(select(stmt), select(stmt)).subquery().select()

        self._assert_result(
            connection,
            u,
            [
                (1,),
            ],
        )


class JoinTest(_JoinTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata, Column("id", Integer, primary_key=True))

        if testing.requires.foreign_keys.enabled:
            # distributed opengauss does NOT support foreign keys
            Table(
                "b",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("a_id", ForeignKey("a.id"), nullable=False),
            )
        else:
            Table(
                "b",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("a_id", Integer, nullable=False),
            )

    @testing.requires.foreign_keys
    def test_inner_join_fk(self):
        a, b = self.tables("a", "b")

        stmt = select(a, b).select_from(a.join(b)).order_by(a.c.id, b.c.id)

        self._assert_result(stmt, [(1, 1, 1), (1, 2, 1), (2, 4, 2), (3, 5, 3)])

    @testing.requires.foreign_keys
    def test_outer_join_fk(self):
        a, b = self.tables("a", "b")

        stmt = select(a, b).select_from(a.join(b)).order_by(a.c.id, b.c.id)

        self._assert_result(stmt, [(1, 1, 1), (1, 2, 1), (2, 4, 2), (3, 5, 3)])


class UnicodeSchemaTest(_UnicodeSchemaTest):
    @classmethod
    def define_tables(cls, metadata):
        global t1, t2, t3

        t1 = Table(
            "unitable1",
            metadata,
            Column("méil", Integer, primary_key=True),
            Column("\u6e2c\u8a66", Integer),
            test_needs_fk=True,
        )
        if testing.requires.foreign_keys.enabled:
            t2 = Table(
                "Unitéble2",
                metadata,
                Column("méil", Integer, primary_key=True, key="a"),
                Column(
                    "\u6e2c\u8a66",
                    Integer,
                    ForeignKey("unitable1.méil"),
                    key="b",
                ),
                test_needs_fk=True,
            )

        else:
            t2 = Table(
                "Unitéble2",
                metadata,
                Column("méil", Integer, primary_key=True, key="a"),
                Column(
                    "\u6e2c\u8a66",
                    Integer,
                    key="b",
                ),
                test_needs_fk=True,
            )

        t3 = Table(
            "\u6e2c\u8a66",
            metadata,
            Column(
                "\u6e2c\u8a66_id",
                Integer,
                primary_key=True,
                autoincrement=False,
            ),
            Column("unitable1_\u6e2c\u8a66", Integer),
            Column("Unitéble2_b", Integer),
            Column("\u6e2c\u8a66_self", Integer),
            test_needs_fk=True,
        )

    def test_insert(self, connection):
        connection.execute(t1.insert(), {"méil": 1, "\u6e2c\u8a66": 5})
        connection.execute(t2.insert(), {"a": 1, "b": 1})
        connection.execute(
            t3.insert(),
            {
                "\u6e2c\u8a66_id": 1,
                "unitable1_\u6e2c\u8a66": 5,
                "Unitéble2_b": 1,
                "\u6e2c\u8a66_self": 1,
            },
        )

        eq_(connection.execute(t1.select()).fetchall(), [(1, 5)])
        eq_(connection.execute(t2.select()).fetchall(), [(1, 1)])
        eq_(connection.execute(t3.select()).fetchall(), [(1, 5, 1, 1)])

    def test_col_targeting(self, connection):
        connection.execute(t1.insert(), {"méil": 1, "\u6e2c\u8a66": 5})
        connection.execute(t2.insert(), {"a": 1, "b": 1})
        connection.execute(
            t3.insert(),
            {
                "\u6e2c\u8a66_id": 1,
                "unitable1_\u6e2c\u8a66": 5,
                "Unitéble2_b": 1,
                "\u6e2c\u8a66_self": 1,
            },
        )

        row = connection.execute(t1.select()).first()
        eq_(row._mapping[t1.c["méil"]], 1)
        eq_(row._mapping[t1.c["\u6e2c\u8a66"]], 5)

        row = connection.execute(t2.select()).first()
        eq_(row._mapping[t2.c["a"]], 1)
        eq_(row._mapping[t2.c["b"]], 1)

        row = connection.execute(t3.select()).first()
        eq_(row._mapping[t3.c["\u6e2c\u8a66_id"]], 1)
        eq_(row._mapping[t3.c["unitable1_\u6e2c\u8a66"]], 5)
        eq_(row._mapping[t3.c["Unitéble2_b"]], 1)
        eq_(row._mapping[t3.c["\u6e2c\u8a66_self"]], 1)

    def test_reflect(self, connection):
        connection.execute(t1.insert(), {"méil": 2, "\u6e2c\u8a66": 7})
        connection.execute(t2.insert(), {"a": 2, "b": 2})
        connection.execute(
            t3.insert(),
            {
                "\u6e2c\u8a66_id": 2,
                "unitable1_\u6e2c\u8a66": 7,
                "Unitéble2_b": 2,
                "\u6e2c\u8a66_self": 2,
            },
        )

        meta = MetaData()
        tt1 = Table(t1.name, meta, autoload_with=connection)
        tt2 = Table(t2.name, meta, autoload_with=connection)
        tt3 = Table(t3.name, meta, autoload_with=connection)

        connection.execute(tt1.insert(), {"méil": 1, "\u6e2c\u8a66": 5})
        connection.execute(tt2.insert(), {"méil": 1, "\u6e2c\u8a66": 1})
        connection.execute(
            tt3.insert(),
            {
                "\u6e2c\u8a66_id": 1,
                "unitable1_\u6e2c\u8a66": 5,
                "Unitéble2_b": 1,
                "\u6e2c\u8a66_self": 1,
            },
        )

        eq_(
            connection.execute(tt1.select().order_by(desc("méil"))).fetchall(),
            [(2, 7), (1, 5)],
        )
        eq_(
            connection.execute(tt2.select().order_by(desc("méil"))).fetchall(),
            [(2, 2), (1, 1)],
        )
        eq_(
            connection.execute(
                tt3.select().order_by(desc("\u6e2c\u8a66_id"))
            ).fetchall(),
            [(2, 7, 2, 2), (1, 5, 1, 1)],
        )


from sqlalchemy import literal_column, func, select, Integer

class TrueDivTest(_TrueDivTest):
    @testing.combinations(
        ("15", "10", 1), ("-15", "5", -3), argnames="left, right, expected"
    )
    def test_floordiv_integer(self, connection, left, right, expected):
        """test #4926"""

        eq_(
            connection.scalar(
                select(
                    func.floor(literal_column(left, type_=Integer()) // literal_column(right, type_=Integer()))

                )
            ),
            expected,
        )

    def test_floordiv_integer_bound(self, connection):
        """test #4926"""

        eq_(
           connection.scalar(select(func.floor(literal(15) // literal(10)))),
            1,
        )

# -*- coding: utf-8 -*-
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# Copyright (C) 2021-2022 Huawei Technologies Co.,Ltd.
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

import sys

from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    @property
    def deferrable_or_no_constraints(self):
        """Target database must support deferrable constraints."""

        return exclusions.open()

    @property
    def check_constraints(self):
        """Target database must support check constraints."""

        return exclusions.open()

    @property
    def enforces_check_constraints(self):
        """Target database must also enforce check constraints."""

        return exclusions.open()

    @property
    def named_constraints(self):
        """target database must support names for constraints."""

        return exclusions.open()

    @property
    def implicitly_named_constraints(self):
        """target database must apply names to unnamed constraints."""

        return exclusions.open()

    @property
    def foreign_keys(self):
        """Target database must support foreign keys."""
        return exclusions.skip_if("opengauss+dc_psycopg2")

    @property
    def foreign_key_constraint_reflection(self):
        return self.foreign_keys

    @property
    def self_referential_foreign_keys(self):
        return self.foreign_keys

    @property
    def foreign_key_constraint_name_reflection(self):
        return self.foreign_keys

    @property
    def table_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for tables."""

        return exclusions.open()

    @property
    def index_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for indexes."""

        return exclusions.closed()

    @property
    def on_update_cascade(self):
        """target database must support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return self.foreign_keys

    @property
    def non_updating_cascade(self):
        """target database must *not* support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return self.foreign_keys

    @property
    def recursive_fk_cascade(self):
        """target database must support ON DELETE CASCADE on a self-referential
        foreign key"""

        return self.foreign_keys

    @property
    def deferrable_fks(self):
        """target database must support deferrable fks"""

        return self.foreign_keys

    @property
    def foreign_key_constraint_option_reflection_ondelete(self):
        return self.foreign_keys

    @property
    def fk_constraint_option_reflection_ondelete_restrict(self):
        return self.foreign_keys

    @property
    def fk_constraint_option_reflection_ondelete_noaction(self):
        return self.foreign_keys

    @property
    def foreign_key_constraint_option_reflection_onupdate(self):
        return self.foreign_keys

    @property
    def fk_constraint_option_reflection_onupdate_restrict(self):
        return self.foreign_keys

    @property
    def comment_reflection(self):
        return exclusions.open()

    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        return exclusions.open()

    @property
    def boolean_col_expressions(self):
        """Target database must support boolean expressions as columns"""
        return exclusions.open()

    @property
    def non_native_boolean_unconstrained(self):
        """target database is not native boolean and allows arbitrary integers
        in it's "bool" column"""

        return exclusions.open()

    @property
    def standalone_binds(self):
        """target database/driver supports bound parameters as column expressions
        without being in the context of a typed column.

        """
        return exclusions.open()

    @property
    def qmark_paramstyle(self):
        return exclusions.closed()

    @property
    def named_paramstyle(self):
        return exclusions.closed()

    @property
    def format_paramstyle(self):
        return exclusions.closed()

    @property
    def pyformat_paramstyle(self):
        return exclusions.open()

    @property
    def no_quoting_special_bind_names(self):
        """Target database will quote bound parameter names, doesn't support
        EXPANDING"""

        return exclusions.open()

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return exclusions.open()

    @property
    def temp_table_reflection(self):
        return self.temporary_tables

    @property
    def temp_table_reflect_indexes(self):
        return exclusions.open()

    @property
    def reflectable_autoincrement(self):
        """Target database must support tables that can automatically generate
        PKs assuming they were reflected.

        this is essentially all the DBs in "identity" plus PostgreSQL, which
        has SERIAL support.  FB and Oracle (and sybase?) require the Sequence
        to be explicitly added, including if the table was reflected.
        """
        return exclusions.open()

    @property
    def insert_from_select(self):
        return exclusions.open()

    @property
    def fetch_rows_post_commit(self):
        return exclusions.open()

    @property
    def non_broken_binary(self):
        """target DBAPI must work fully with binary values"""

        return exclusions.open()

    @property
    def binary_comparisons(self):
        """target database/driver can allow BLOB/BINARY fields to be compared
        against a bound parameter value.
        """
        return exclusions.open()

    @property
    def binary_literals(self):
        """target backend supports simple binary literals, e.g. an
        expression like::

            SELECT CAST('foo' AS BINARY)

        Where ``BINARY`` is the type emitted from :class:`.LargeBinary`,
        e.g. it could be ``BLOB`` or similar.

        Basically fails on Oracle.

        """

        return exclusions.open()

    @property
    def tuple_in(self):
        return exclusions.open()

    @property
    def tuple_in_w_empty(self):
        return exclusions.open()

    @property
    def independent_cursors(self):
        """Target must support simultaneous, independent database cursors
        on a single connection."""

        return exclusions.open()

    @property
    def cursor_works_post_rollback(self):
        """Driver quirk where the cursor.fetchall() will work even if
        the connection has been rolled back.

        This generally refers to buffered cursors but also seems to work
        with cx_oracle, for example.

        """

        return exclusions.open()

    @property
    def independent_connections(self):
        """
        Target must support simultaneous, independent database connections.
        """

        return exclusions.open()

    @property
    def memory_process_intensive(self):
        """Driver is able to handle the memory tests which run in a subprocess
        and iterate through hundreds of connections

        """
        return exclusions.open()

    @property
    def updateable_autoincrement_pks(self):
        """Target must support UPDATE on autoincrement/integer primary key."""

        return exclusions.open()

    @property
    def isolation_level(self):
        return exclusions.open()

    @property
    def legacy_isolation_level(self):
        return exclusions.open()

    def get_isolation_levels(self, config):
        levels = set(config.db.dialect._isolation_lookup)
        default = "READ COMMITTED"
        levels.add("AUTOCOMMIT")

        return {"default": default, "supported": levels}

    @property
    def autocommit(self):
        """target dialect supports 'AUTOCOMMIT' as an isolation_level"""

        return exclusions.open()

    @property
    def row_triggers(self):
        """Target must support standard statement-running EACH ROW triggers."""

        return exclusions.open()

    @property
    def sequences_as_server_defaults(self):
        """Target database must support SEQUENCE as a server side default."""

        return exclusions.open()

    @property
    def sql_expressions_inserted_as_primary_key(self):
        return exclusions.open()

    @property
    def computed_columns_on_update_returning(self):
        return exclusions.closed()

    @property
    def correlated_outer_joins(self):
        """Target must support an outer join to a subquery which
        correlates to the parent."""

        return exclusions.open()

    @property
    def multi_table_update(self):
        return exclusions.closed()

    @property
    def update_from(self):
        """Target must support UPDATE..FROM syntax"""

        return exclusions.open()

    @property
    def delete_from(self):
        """Target must support DELETE FROM..FROM or DELETE..USING syntax"""
        return exclusions.open()

    @property
    def update_where_target_in_subquery(self):
        """Target must support UPDATE (or DELETE) where the same table is
        present in a subquery in the WHERE clause.

        This is an ANSI-standard syntax that apparently MySQL can't handle,
        such as::

            UPDATE documents SET flag=1 WHERE documents.title IN
                (SELECT max(documents.title) AS title
                    FROM documents GROUP BY documents.user_id
                )

        """
        return exclusions.open()

    @property
    def savepoints(self):
        """Target database must support savepoints."""

        return exclusions.open()

    @property
    def savepoints_w_release(self):
        return exclusions.open()

    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return exclusions.open()

    @property
    def cross_schema_fk_reflection(self):
        """target system must support reflection of inter-schema foreign
        keys"""
        return self.foreign_keys

    @property
    def implicit_default_schema(self):
        """target system has a strong concept of 'default' schema that can
        be referred to implicitly.

        basically, PostgreSQL.

        """
        return exclusions.open()

    @property
    def default_schema_name_switch(self):
        return exclusions.open()

    @property
    def unique_constraint_reflection(self):
        return exclusions.open()

    @property
    def unique_constraint_reflection_no_index_overlap(self):
        return exclusions.open()

    @property
    def check_constraint_reflection(self):
        return exclusions.open()

    @property
    def indexes_with_expressions(self):
        return exclusions.open()

    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""

        return exclusions.closed()

    @property
    def temporary_views(self):
        """target database supports temporary views"""
        return exclusions.open()

    @property
    def table_value_constructor(self):
        return exclusions.open()

    @property
    def update_nowait(self):
        """Target database must support SELECT...FOR UPDATE NOWAIT"""
        return exclusions.open()

    @property
    def subqueries(self):
        """Target database must support subqueries."""
        return exclusions.open()

    @property
    def ctes(self):
        """Target database supports CTEs"""
        return exclusions.open()

    @property
    def ctes_with_update_delete(self):
        """target database supports CTES that ride on top of a normal UPDATE
        or DELETE statement which refers to the CTE in a correlated subquery.

        """
        return exclusions.open()

    @property
    def ctes_on_dml(self):
        """target database supports CTES which consist of INSERT, UPDATE
        or DELETE *within* the CTE, e.g. WITH x AS (UPDATE....)"""

        return exclusions.open()

    @property
    def mod_operator_as_percent_sign(self):
        """target database must use a plain percent '%' as the 'modulus'
        operator."""

        return exclusions.open()

    @property
    def intersect(self):
        """Target database must support INTERSECT or equivalent."""

        return exclusions.open()

    @property
    def except_(self):
        """Target database must support EXCEPT or equivalent (i.e. MINUS)."""
        return exclusions.open()

    @property
    def dupe_order_by_ok(self):
        """target db wont choke if ORDER BY specifies the same expression
        more than once

        """

        return exclusions.open()

    @property
    def order_by_col_from_union(self):
        """target database supports ordering by a column from a SELECT
        inside of a UNION

        E.g.  (SELECT id, ...) UNION (SELECT id, ...) ORDER BY id

        Fails on SQL Server

        """
        return exclusions.open()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when LIMIT/OFFSET is specifically present.

        E.g. (SELECT ... LIMIT ..) UNION (SELECT .. OFFSET ..)

        This is known to fail on SQLite.

        """
        return exclusions.open()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when OFFSET/LIMIT is specifically not present.

        E.g. (SELECT ...) UNION (SELECT ..)

        This is known to fail on SQLite.  It also fails on Oracle
        because without LIMIT/OFFSET, there is currently no step that
        creates an additional subquery.

        """
        return exclusions.open()

    @property
    def offset(self):
        """Target database must support some method of adding OFFSET or
        equivalent to a result set."""
        return exclusions.open()

    @property
    def sql_expression_limit_offset(self):
        return exclusions.open()

    @property
    def window_functions(self):
        return exclusions.open()

    @property
    def two_phase_transactions(self):
        """Target database must support two-phase transactions."""

        return exclusions.open()

    @property
    def two_phase_recovery(self):
        return exclusions.open()

    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.open()

    @property
    def empty_strings_varchar(self):
        """
        target database can persist/return an empty string with a varchar.
        """
        # opengauss treat empty string as NULL
        return exclusions.closed()

    @property
    def empty_strings_text(self):
        """target database can persist/return an empty string with an
        unbounded text."""
        # opengauss treat empty string as NULL
        return exclusions.closed()

    @property
    def empty_inserts_executemany(self):
        return exclusions.open()

    @property
    def expressions_against_unbounded_text(self):
        """target database supports use of an unbounded textual field in a
        WHERE clause."""

        return exclusions.open()

    @property
    def unicode_data(self):
        """target drive must support unicode data stored in columns."""
        return exclusions.open()

    @property
    def unicode_connections(self):
        """
        Target driver must support some encoding of Unicode across the wire.

        """
        return exclusions.open()

    @property
    def unicode_ddl(self):
        """Target driver must support some degree of non-ascii symbol names."""

        return exclusions.open()

    @property
    def symbol_names_w_double_quote(self):
        """Target driver can create tables with a name like 'some " table'"""

        return exclusions.open()

    @property
    def emulated_lastrowid(self):
        """ "target dialect retrieves cursor.lastrowid or an equivalent
        after an insert() construct executes.
        """
        return exclusions.closed()

    @property
    def emulated_lastrowid_even_with_sequences(self):
        """ "target dialect retrieves cursor.lastrowid or an equivalent
        after an insert() construct executes, even if the table has a
        Sequence on it.
        """
        return exclusions.closed()

    @property
    def implements_get_lastrowid(self):
        return exclusions.open()

    @property
    def dbapi_lastrowid(self):
        """ "target backend includes a 'lastrowid' accessor on the DBAPI
        cursor object.

        """
        return exclusions.closed()

    @property
    def nullsordering(self):
        """Target backends that support nulls ordering."""
        return exclusions.open()

    @property
    def reflects_pk_names(self):
        """Target driver reflects the name of primary key constraints."""

        return exclusions.open()

    @property
    def nested_aggregates(self):
        """target database can select an aggregate from a subquery that's
        also using an aggregate"""

        return exclusions.open()

    @property
    def tuple_valued_builtin_functions(self):
        return exclusions.open()

    @property
    def array_type(self):
        return exclusions.open()

    @property
    def json_type(self):
        return exclusions.closed()

    @property
    def json_index_supplementary_unicode_element(self):
        return exclusions.open()

    @property
    def legacy_unconditional_json_extract(self):
        """Backend has a JSON_EXTRACT or similar function that returns a
        valid JSON string in all cases.

        Used to test a legacy feature and is not needed.

        """
        return exclusions.open()

    @property
    def sqlite_memory(self):
        return exclusions.closed()

    @property
    def reflects_json_type(self):
        return exclusions.closed()

    @property
    def json_array_indexes(self):
        return self.json_type

    @property
    def datetime_literals(self):
        """target dialect supports rendering of a date, time, or datetime as a
        literal string, e.g. via the TypeEngine.literal_processor() method.

        """

        return exclusions.closed()

    @property
    def datetime(self):
        """target dialect supports representation of Python
        datetime.datetime() objects."""

        return exclusions.open()

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return exclusions.open()

    @property
    def timestamp_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects but only
        if TIMESTAMP is used."""

        return exclusions.open()

    @property
    def datetime_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1900) values."""

        return exclusions.open()

    @property
    def date(self):
        """target dialect supports representation of Python
        datetime.date() objects."""

        return exclusions.closed()

    @property
    def date_coerces_from_datetime(self):
        """target dialect accepts a datetime object as the target
        of a date column."""

        return exclusions.closed()

    @property
    def date_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1900) values."""

        return exclusions.closed()

    @property
    def time(self):
        """target dialect supports representation of Python
        datetime.time() objects."""

        return exclusions.closed()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()

    @property
    def precision_numerics_general(self):
        """target backend has general support for moderately high-precision
        numerics."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        """target backend supports Decimal() objects using E notation
        to represent very small values."""
        # NOTE: this exclusion isn't used in current tests.
        return exclusions.open()

    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values."""

        return exclusions.open()

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """

        return exclusions.open()

    @property
    def cast_precision_numerics_many_significant_digits(self):
        """same as precision_numerics_many_significant_digits but within the
        context of a CAST statement (hello MySQL)

        """
        return exclusions.open()

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return exclusions.open()

    @property
    def infinity_floats(self):
        return exclusions.open()

    @property
    def precision_generic_float_type(self):
        """target backend will return native floating point numbers with at
        least seven decimal places when using the generic Float type."""

        return exclusions.open()

    @property
    def floats_to_four_decimals(self):
        return exclusions.open()

    @property
    def implicit_decimal_binds(self):
        """target backend will return a selected Decimal as a Decimal, not
        a string.

        e.g.::

            expr = decimal.Decimal("15.7563")

            value = e.scalar(
                select(literal(expr))
            )

            assert value == expr

        See :ticket:`4036`

        """

        return exclusions.open()

    @property
    def fetch_null_from_numeric(self):
        return exclusions.open()

    @property
    def duplicate_key_raises_integrity_error(self):
        return exclusions.open()

    @property
    def hstore(self):
        return exclusions.open()

    @property
    def btree_gist(self):
        return exclusions.open()

    @property
    def range_types(self):
        return exclusions.open()

    @property
    def async_dialect(self):
        """dialect makes use of await_() to invoke operations on the DBAPI."""

        return exclusions.closed()

    @property
    def postgresql_jsonb(self):
        return exclusions.closed()

    @property
    def psycopg2_native_hstore(self):
        return self.psycopg2_compatibility

    @property
    def psycopg2_compatibility(self):
        return exclusions.open()

    @property
    def psycopg2_or_pg8000_compatibility(self):
        return exclusions.open()

    @property
    def percent_schema_names(self):
        return exclusions.open()

    @property
    def order_by_label_with_expression(self):
        return exclusions.closed()

    def get_order_by_collation(self, config):
        return "POSIX"

    @property
    def legacy_engine(self):
        return exclusions.open()

    @property
    def ad_hoc_engines(self):
        return exclusions.open()

    @property
    def no_asyncio(self):

        return exclusions.open()

    @property
    def pyodbc_fast_executemany(self):
        return exclusions.closed()

    @property
    def python_fixed_issue_8743(self):
        return exclusions.skip_if(
            lambda: sys.version_info < (2, 7, 8),
            "Python issue 8743 fixed in Python 2.7.8",
        )

    @property
    def granular_timezone(self):
        """the datetime.timezone class, or SQLAlchemy's port, supports
        seconds and microseconds.

        SQLAlchemy ported the Python 3.7 version for Python 2, so
        it passes on that.  For Python 3.6 and earlier, it is not supported.

        """
        return exclusions.skip_if(
            lambda: sys.version_info >= (3,) and sys.version_info < (3, 7)
        )

    @property
    def selectone(self):
        """target driver must support the literal statement 'select 1'"""
        return exclusions.open()

    @property
    def postgresql_utf8_server_encoding(self):
        return exclusions.open()

    @property
    def computed_columns(self):
        return exclusions.closed()

    @property
    def python_profiling_backend(self):
        return exclusions.closed()

    @property
    def computed_columns_stored(self):
        return exclusions.open()

    @property
    def computed_columns_virtual(self):
        return exclusions.closed()

    @property
    def computed_columns_default_persisted(self):
        return exclusions.open()

    @property
    def computed_columns_reflect_persisted(self):
        return exclusions.open()

    @property
    def regexp_match(self):
        return exclusions.open()

    @property
    def regexp_replace(self):
        return exclusions.open()

    @property
    def supports_distinct_on(self):
        """If a backend supports the DISTINCT ON in a select"""
        return exclusions.open()

    @property
    def supports_for_update_of(self):
        return exclusions.closed()

    @property
    def sequences_in_other_clauses(self):
        """sequences allowed in WHERE, GROUP BY, HAVING, etc."""
        return exclusions.open()

    @property
    def supports_lastrowid_for_expressions(self):
        """cursor.lastrowid works if an explicit SQL expression was used."""
        return exclusions.closed()

    @property
    def supports_sequence_for_autoincrement_column(self):
        """for mssql, autoincrement means IDENTITY, not sequence"""
        return exclusions.open()

    @property
    def identity_columns(self):
        return exclusions.closed()

    @property
    def identity_columns_standard(self):
        return exclusions.closed()

    @property
    def index_reflects_included_columns(self):
        return exclusions.closed()

    @property
    def fetch_first(self):
        return exclusions.open()

    @property
    def fetch_percent(self):
        return exclusions.closed()

    @property
    def fetch_ties(self):
        return exclusions.closed()

    @property
    def fetch_no_order_by(self):
        return exclusions.open()

    @property
    def fetch_offset_with_options(self):
        # use together with fetch_first
        return exclusions.open()

    @property
    def fetch_expression(self):
        # use together with fetch_first
        return exclusions.open()

    @property
    def autoincrement_without_sequence(self):
        return exclusions.open()

    @property
    def reflect_tables_no_columns(self):
        # opengauss doesn't support this
        return exclusions.closed()

    @property
    def limit_with_strict_isotonicity(self):
        """`LIMIT` in distributed opengauss does not have strict isotonicity.

        Use `limit` with `order_by` if you need strict isotonicity.
        """
        return exclusions.only_on(["opengauss+psycopg2"])

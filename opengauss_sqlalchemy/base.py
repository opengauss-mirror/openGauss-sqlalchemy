# -*- coding: utf-8 -*-
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# Copyright (C) 2021-2022 Huawei Technologies Co.,Ltd.
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from sqlalchemy.dialects.postgresql.base import IDX_USING, PGCompiler, PGDDLCompiler, PGIdentifierPreparer
from sqlalchemy.dialects.postgresql.base import RESERVED_WORDS as _RESERVED_WORDS
from sqlalchemy.sql import coercions, expression, roles, elements
from sqlalchemy import types


_distributable_types = (
    types.BIGINT, types.BigInteger, types.INT, types.INTEGER,
    types.Integer, types.NUMERIC, types.Numeric, types.SMALLINT,
    types.SmallInteger, types.DECIMAL,
    types.CHAR, types.NVARCHAR, types.String, types.TEXT,
    types.Text, types.Unicode, types.UnicodeText, types.VARCHAR,
    types.DATE, types.DATETIME, types.Date, types.DateTime,
    types.Interval, types.TIME, types.TIMESTAMP, types.Time,
)

_undistributable_types = (
    "FLOAT",  # FLOAT isinstance of types.Numeric/DECIMAL
)

# Ref: https://opengauss.org/en/docs/3.1.0/docs/Developerguide/keywords.html
RESERVED_WORDS = _RESERVED_WORDS.union(
    [
        "authid",
        "buckets",
        "excluded",
        "fenced",
        "groupparent",
        "is",
        "less",
        "maxvalue",
        "minus",
        "modify",
        "performance",
        "priorer",
        "procedure",
        "reject",
        "rownum",
        "sysdate",
        "verify",
    ]
)


class OpenGaussCompiler(PGCompiler):
    def get_cte_preamble(self, recursive):
        return "WITH RECURSIVE"

    def visit_on_conflict_do_nothing(self, on_conflict, **kw):
        return "ON DUPLICATE KEY UPDATE NOTHING"

    def visit_on_conflict_do_update(self, on_conflict, **kw):
        clause = on_conflict

        target_text = self._on_conflict_target(on_conflict, **kw)

        action_set_ops = []

        set_parameters = dict(clause.update_values_to_set)
        # create a list of column assignment clauses as tuples

        insert_statement = self.stack[-1]["selectable"]
        cols = insert_statement.table.c
        for c in cols:
            col_key = c.key

            if col_key in set_parameters:
                value = set_parameters.pop(col_key)
            elif c in set_parameters:
                value = set_parameters.pop(c)
            else:
                continue

            if coercions._is_literal(value):
                value = elements.BindParameter(None, value, type_=c.type)

            else:
                if (
                        isinstance(value, elements.BindParameter)
                        and value.type._isnull
                ):
                    value = value._clone()
                    value.type = c.type
            value_text = self.process(value.self_group(), use_schema=False)

            key_text = self.preparer.quote(c.name)
            action_set_ops.append("%s = %s" % (key_text, value_text))

        # check for names that don't match columns
        if set_parameters:
            util.warn(
                "Additional column names not matching "
                "any column keys in table '%s': %s"
                % (
                    self.current_executable.table.name,
                    (", ".join("'%s'" % c for c in set_parameters)),
                )
            )
            for k, v in set_parameters.items():
                key_text = (
                    self.preparer.quote(k)
                    if isinstance(k, str)
                    else self.process(k, use_schema=False)
                )
                value_text = self.process(
                    coercions.expect(roles.ExpressionElementRole, v),
                    use_schema=False,
                )
                action_set_ops.append("%s = %s" % (key_text, value_text))

        action_text = ", ".join(action_set_ops)
        if clause.update_whereclause is not None:
            action_text += " WHERE %s" % self.process(
                clause.update_whereclause, include_table=True, use_schema=False
            )

        return "ON DUPLICATE KEY UPDATE %s" % (action_text)


class OpenGaussDDLCompiler(PGDDLCompiler):
    """DDLCompiler for opengauss"""

    def visit_create_index(self, create, **kw):
        preparer = self.preparer
        index = create.element
        self._verify_index_table(index)
        text_contents = ["CREATE "]
        if index.unique:
            text_contents.append("UNIQUE ")
        text_contents.append("INDEX ")

        if self.dialect._supports_create_index_concurrently:
            concurrently = index.dialect_options["opengauss"]["concurrently"]
            if concurrently:
                text_contents.append("CONCURRENTLY ")

        text_contents.append(
            "%s ON %s " % (
                self._prepared_index_name(index, include_schema=False),
                preparer.format_table(index.table),
            )
        )

        using = index.dialect_options["opengauss"]["using"]
        if using:
            text_contents.append(
                "USING %s "
                % self.preparer.validate_sql_phrase(using, IDX_USING).lower()
            )

        ops = index.dialect_options["opengauss"]["ops"]
        text_contents.append(
            "(%s)" % (
                ", ".join(
                    [
                        self.sql_compiler.process(
                            expr.self_group()
                            if not isinstance(expr, expression.ColumnClause) else expr,
                            include_table=False,
                            literal_binds=True,
                        )
                        + (
                            (" " + ops[expr.key])
                            if hasattr(expr, "key") and expr.key in ops else ""
                        )
                        for expr in index.expressions
                    ]
                )
            )
        )

        local = index.dialect_options["opengauss"]["local"]
        if local:
            text_contents.append(" LOCAL")
            if local[0]:
                text_contents.append(
                    " (%s)" % (
                        ", ".join([local_partition for local_partition in local])
                    )
                )

        withclause = index.dialect_options["opengauss"]["with"]
        if withclause:
            text_contents.append(
                " WITH (%s)" % (
                    ", ".join(
                        [
                            "%s = %s" % storage_parameter
                            for storage_parameter in withclause.items()
                        ]
                    )
                )
            )

        tablespace_name = index.dialect_options["opengauss"]["tablespace"]
        if tablespace_name:
            text_contents.append(" TABLESPACE %s" % preparer.quote(tablespace_name))

        whereclause = index.dialect_options["opengauss"]["where"]
        if whereclause is not None:
            whereclause = coercions.expect(
                roles.DDLExpressionRole, whereclause
            )

            where_compiled = self.sql_compiler.process(
                whereclause, include_table=False, literal_binds=True
            )
            text_contents.append(" WHERE " + where_compiled)

        return "".join(text_contents)

    def visit_drop_index(self, drop, **kw):
        index = drop.element

        text_contents = ["\nDROP INDEX "]

        if self.dialect._supports_drop_index_concurrently:
            concurrently = index.dialect_options["opengauss"]["concurrently"]
            if concurrently:
                text_contents.append("CONCURRENTLY ")

        if drop.if_exists:
            text_contents.append("IF EXISTS ")

        text_contents.append(self._prepared_index_name(index, include_schema=True))
        return "".join(text_contents)

    def post_create_table(self, table):
        table_opts = []
        gauss_opts = table.dialect_options["opengauss"]

        if gauss_opts["with"]:
            table_opts.append(
                "\n WITH (%s)" % (
                    ", ".join(
                        ["%s = %s" % storage_parameter for storage_parameter in gauss_opts["with"].items()]
                    )
                )
            )

        if gauss_opts["on_commit"]:
            table_opts.append("\n ON COMMIT %s" % gauss_opts["on_commit"])

        if gauss_opts["compress"]:
            table_opts.append("\n COMPRESS")

        if gauss_opts["tablespace"]:
            tablespace_name = gauss_opts["tablespace"]
            table_opts.append(
                "\n TABLESPACE %s" % self.preparer.quote(tablespace_name)
            )

        if self.dialect._supports_table_distribute_by:
            if gauss_opts["distribute_by"]:
                # Support DISTRIBUTE BY in distributed opengauss.
                # Usage: `Table("some_table", opengauss_distribute_by='HASH(column_name)')`
                # See https://support.huaweicloud.com/devg-opengauss/opengauss_devg_0402.html
                table_opts.append("\n DISTRIBUTE BY %s" % gauss_opts["distribute_by"])
            else:
                for col in table.columns:
                    if isinstance(col.type, _distributable_types) and str(col.type) not in _undistributable_types:
                        break
                else:
                    # treat table without distributable columns as REPLICATION in distributed opengauss
                    table_opts.append("\n DISTRIBUTE BY REPLICATION")

        if gauss_opts["to"]:
            table_opts.append("\n TO %s" % gauss_opts["to"])

        if gauss_opts["partition_by"]:
            table_opts.append("\n PARTITION BY %s" % gauss_opts["partition_by"])

        if gauss_opts["enable_row_movement"]:
            table_opts.append("\n ENABLE ROW MOVEMENT")

        return "".join(table_opts)


class OpenGaussIdentifierPreparer(PGIdentifierPreparer):
    reserved_words = RESERVED_WORDS

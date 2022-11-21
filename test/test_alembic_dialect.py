# -*- coding: utf-8 -*-
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# Copyright (C) 2021-2022 Huawei Technologies Co.,Ltd.
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
from alembic import migration
from alembic.ddl.base import RenameTable
from alembic.ddl.postgresql import PostgresqlColumnType
from sqlalchemy import types
from sqlalchemy.testing import fixtures

from opengauss_sqlalchemy import psycopg2


class AlembicDialectTest(fixtures.TestBase):

    def test_configure_migration_context(self):
        context = migration.MigrationContext.configure(
            url='opengauss+psycopg2://mydb'
        )
        assert isinstance(context.impl, psycopg2.OpenGaussImpl)

    def test_rename_table(self):
        compiler = psycopg2.OpenGaussDDLCompiler(psycopg2.dialect(), None)
        sql = compiler.process(RenameTable("old", "new", "schema"))
        assert sql == 'ALTER TABLE schema."old" RENAME TO "new"'

    def test_alter_column_type(self):
        compiler = psycopg2.OpenGaussDDLCompiler(psycopg2.dialect(), None)
        sql = compiler.process(
            PostgresqlColumnType("table_name", "column_name", types.INTEGER)
        )
        assert sql == "ALTER TABLE table_name ALTER COLUMN column_name TYPE INTEGER "
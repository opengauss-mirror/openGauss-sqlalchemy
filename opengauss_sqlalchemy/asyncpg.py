# -*- coding: utf-8 -*-
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# Copyright (C) 2025-2025 Huawei Technologies Co.,Ltd.
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php


from __future__ import annotations

import collections
import decimal
import json as _py_json
import re
import time
from typing import cast
from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql import json, ranges
from sqlalchemy.dialects.postgresql.array import ARRAY as PGARRAY
from sqlalchemy.dialects.postgresql.base import *
from sqlalchemy.dialects.postgresql.types import *
from sqlalchemy.dialects.postgresql.asyncpg import *

from sqlalchemy import exc, pool, util

from sqlalchemy.engine import AdaptedConnection, processors
from sqlalchemy.sql import sqltypes
from sqlalchemy.util.concurrency import asyncio, await_fallback, await_only

from opengauss_sqlalchemy.base import OpenGaussIdentifierPreparer, OpenGaussCompiler

if TYPE_CHECKING:
    from typing import Iterable


class OpenGaussExecutionContext_asyncpg(PGExecutionContext_asyncpg):
    pass

class OpenGaussCompiler_asyncpg(OpenGaussCompiler):
    pass

class OpenGaussIdentifierPreparer_asyncpg(OpenGaussIdentifierPreparer):
    pass

class OpenGaussDialect_asyncpg(PGDialect_asyncpg):
    driver = "asyncpg"
    supports_statement_cache = True

    supports_server_side_cursors = True

    render_bind_cast = True
    has_terminate = True

    default_paramstyle = "numeric_dollar"
    supports_sane_multi_rowcount = False

    execution_ctx_cls = OpenGaussExecutionContext_asyncpg
    statement_compiler = OpenGaussCompiler_asyncpg
    preparer = OpenGaussIdentifierPreparer_asyncpg

    def _get_server_version_info(self, connection):
        # most of opengauss features are same with postgres 9.2.4
        return (9, 2, 4)

dialect = OpenGaussDialect_asyncpg

# -*- coding: utf-8 -*-
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# Copyright (C) 2021-2022 Huawei Technologies Co.,Ltd.
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from opengauss_sqlalchemy.psycopg2 import OpenGaussDialect_psycopg2


class OpenGaussDialect_dc_psycopg2(OpenGaussDialect_psycopg2):
    name = "opengauss"
    driver = "dc_psycopg2"

    supports_statement_cache = True

    _supports_create_index_concurrently = False
    _supports_drop_index_concurrently = False
    _supports_table_distribute_by = True


dialect = OpenGaussDialect_dc_psycopg2

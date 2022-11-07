# opengauss_sqlalchemy/__init__.py
# Copyright (C) 2021-2022 Huawei.
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from sqlalchemy.dialects import registry


__version__ = "2.0"


registry.register(
    "opengauss", "opengauss_sqlalchemy.psycopg2", "OpenGaussDialect_psycopg2"
)
registry.register(
    "opengauss.psycopg2", "opengauss_sqlalchemy.psycopg2", "OpenGaussDialect_psycopg2"
)
registry.register(
    "opengauss.dc_psycopg2", "opengauss_sqlalchemy.dc_psycopg2", "OpenGaussDialect_dc_psycopg2"
)

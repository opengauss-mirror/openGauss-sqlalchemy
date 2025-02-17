import os
from pathlib import Path
import re

from setuptools import find_namespace_packages
from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), "opengauss_sqlalchemy", "__init__.py")) as fd:
    VERSION = re.compile(r'.*__version__ = "(.*?)"', re.S).match(fd.read()).group(1)

long_description = (Path(__file__).parent / "README.md").read_text()


setup(
    name="opengauss-sqlalchemy",
    version=VERSION,
    description="OpenGauss Dialect for SQLAlchemy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Jia Junsu",
    author_email="jiajunsu@huawei.com",
    url="https://gitee.com/opengauss/openGauss-sqlalchemy",
    license="MIT",
    classifiers=[
        "Development Status :: 5 - PRODUCTION/STABLE",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database :: Front-Ends",
    ],
    packages=find_namespace_packages(exclude=["test"]),
    include_package_data=True,
    install_requires=["sqlalchemy<=2.0.23"],
    entry_points={
        "sqlalchemy.dialects": [
            "opengauss = opengauss_sqlalchemy.psycopg2:OpenGaussDialect_psycopg2",
            "opengauss.psycopg2 = opengauss_sqlalchemy.psycopg2:OpenGaussDialect_psycopg2",
            "opengauss.dc_psycopg2 = opengauss_sqlalchemy.dc_psycopg2:OpenGaussDialect_dc_psycopg2",
        ]
    },
)

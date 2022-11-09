import os
import re

from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), "opengauss_sqlalchemy", "__init__.py")) as fd:
    VERSION = re.compile(r'.*__version__ = "(.*?)"', re.S).match(fd.read()).group(1)


setup(
    name="opengauss-sqlalchemy",
    version=VERSION,
    description="OpenGauss Dialect for SQLAlchemy",
    author="Jia Junsu",
    author_email="jiajunsu@huawei.com",
    url="https://gitee.com/opengauss/openGauss-sqlalchemy",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database :: Front-Ends",
    ],
    packages=["opengauss_sqlalchemy"],
    include_package_data=True,
    install_requires=["SQLAlchemy<2.0", "psycopg2>=2.8"],
    entry_points={
        "sqlalchemy.dialects": [
            "opengauss = opengauss_sqlalchemy.psycopg2:OpenGaussDialect_psycopg2",
            "opengauss.psycopg2 = opengauss_sqlalchemy.psycopg2:OpenGaussDialect_psycopg2",
            "opengauss.dc_psycopg2 = opengauss_sqlalchemy.dc_psycopg2:OpenGaussDialect_dc_psycopg2",
        ]
    },
)

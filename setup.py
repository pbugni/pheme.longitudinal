#!/usr/bin/env python

import os
from setuptools import setup

docs_require = ['Sphinx']
tests_require = ['nose', 'coverage']

try:
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, 'README.txt')) as r:
        README = r.read()
except IOError:
    README = ''

setup(name='pheme.longitudinal',
      version='13.12',
      description="PHEME Longitudinal Module",
      long_description=README,
      license="BSD-3 Clause",
      namespace_packages=['pheme'],
      packages=['pheme.longitudinal', ],
      include_package_data=True,
      install_requires=['setuptools', 'pheme.util', 'psycopg2', 'PyYAML',
	'SQLAlchemy'],
      setup_requires=['nose'],
      tests_require=tests_require,
      test_suite="nose.collector",
      extras_require = {'test': tests_require,
                        'docs': docs_require,
                        },
      entry_points=("""
                    [console_scripts]
                    create_longitudinal_tables=pheme.longitudinal.tables:main
                    load_static_data=pheme.longitudinal.static_data:load
                    dump_static_data=pheme.longitudinal.static_data:dump
                    """),
)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
    from setuptools.command.install import install
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup
    from setuptools.command.install import install

import os
import sys

sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), os.pardir))
import suite

class no_install(install):

    def run(self, *args, **kwargs):
        import sys
        sys.stderr.write("""
------------------------------------------------------
The Cyme functional test suite cannot be installed.
------------------------------------------------------


But you can execute the tests by running the command:

    $ python setup.py test


""")


setup(
    name='cyme-funtests',
    version="DEV",
    description="Functional test suite for Cyme",
    author="Ask Solem",
    author_email="ask@rabbitmq.com",
    url="--",
    platforms=["any"],
    packages=[],
    data_files=[],
    zip_safe=False,
    cmdclass={"install": no_install},
    test_suite="nose.collector",
    tests_require=[
        "unittest2>=0.4.0",
        "simplejson",
        "nose",
    ],
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
    ],
    long_description="Do not install this package",
)

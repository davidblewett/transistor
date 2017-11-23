#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.0',
    # TODO: put package requirements here
    'tornado',
    'zope.interface',
    'vmprof',
]

setup_requirements = [
    # TODO(davidblewett): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    # TODO: put package test requirements here
    'mock',
]

setup(
    name='transistor',
    version='0.1.0',
    description="Match disparate input sources with an output destination.",
    long_description=readme + '\n\n' + history,
    author="David Blewett",
    author_email='david@dawninglight.net',
    url='https://github.com/davidblewett/transistor',
    packages=find_packages(include=['transistor']),
    entry_points={
        'console_scripts': [
            'actuator=transistor.scripts.actuator:main',
            'transistor=transistor.cli:main'
        ]
    },
    extras_require={
        #":python_version<'3.0'": ["futures", "monotonic"],
        'AWS':  ["botocore"],
        'Kafka':  ["confluent_kafka"],
        'ZMQ':  ["pyzmq"],
    },
    include_package_data=True,
    install_requires=requirements,
    license="BSD license",
    zip_safe=False,
    keywords='transistor',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests.test_collector',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)

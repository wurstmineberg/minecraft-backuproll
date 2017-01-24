#!/usr/bin/env python

#from distutils.core import setup

#!/usr/bin/env python

from setuptools import setup

setup(name='minecraft-backuproll',
        #version='1.0',
        description='A cron script to create and manage backups for minecraft servers',
        author='Wurstmineberg',
        author_email='mail@wurstmineberg.de',
        #py_modules=["backuproll"],
        packages=["backuproll"],
        package_data={"backuproll": ["assets/*.json"]},
        use_scm_version = {
            "write_to": "backuproll/_version.py",
        },
        setup_requires=["setuptools_scm"],
        install_requires=[
            "docopt",
            "setuptools-scm",
            "byte_fifo",
        ],
        dependency_links=[
            "git+https://github.com/wurstmineberg/byte-fifo.git#egg=byte_fifo-0.1_alpha"
        ]
    )

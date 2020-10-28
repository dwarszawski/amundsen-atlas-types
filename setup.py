__version__ = '1.1.4'

import os
import sys

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, here)

with open(os.path.join(here, 'README.md')) as readme_file:
    readme = readme_file.read()


requirements_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'requirements.txt')
with open(requirements_path) as requirements_file:
    requirements = requirements_file.readlines()

setup_args = dict(
    name='amundsenatlastypes',
    version=__version__,
    description=('Custom Amundsen Atlas data types definition'),
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Damian Warszawski",
    maintainer="Verdan Mahmood",
    maintainer_email='verdan.mahmood@gmail.com',
    url='https://github.com/dwarszawski/amundsen-atlas-types',
    packages=find_packages(include=['amundsenatlastypes']),
    include_package_data=True,
    install_requires=requirements,
    license='Apache Software License 2.0',
    zip_safe=False,
    keywords='apache atlas, atlas types, amundsen, amundsen atlas',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)


setup(**setup_args)

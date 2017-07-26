from debian import changelog
from setuptools import setup

__version__ = str(changelog.Changelog(open('debian/changelog')).version)
__desc__ = 'Orchestrator next-gen prototype'

setup(
    name='cocaine-orca',
    version=__version__,
    packages=[
        'burlak',
        'burlak.sec',
        'burlak.tests',
        'burlak.tests.sched_load'
    ],
    url='',
    license='',
    author='karapuz',
    author_email='karapuz@yandex-team.ru',
    install_requires=[
        'tornado>=4.3',
        'click>=5.0',
        'PyYAML>=3.0',
    ],
    description=__desc__
)

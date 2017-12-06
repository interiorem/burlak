from debian import changelog

from setuptools import find_packages, setup


__version__ = str(changelog.Changelog(open('debian/changelog')).version)
__desc__ = 'Orchestrator next-gen prototype'


setup(
    name='cocaine-orca',
    version=__version__,
    packages=find_packages('src'),
    url='https://github.com/karitra/burlak',
    license='Copyleft',
    author='Alex Karev',
    author_email='karapuz@yandex-team.ru',
    install_requires=[
        # 'cocaine',
        'tornado>=4.3',
        'click>=5.0',
        'PyYAML>=3.0',
        'cerberus>=0.9.0',
        'raven>=5.0.0',
    ],
    namespace_packages=['cocaine'],
    setup_requires=['pytest-runner', 'python-debian'],
    tests_require=['pytest'],
    package_dir={'': 'src'},
    description=__desc__,
    package_data={'': ["debian/changelog"]},
    include_package_data=True
)

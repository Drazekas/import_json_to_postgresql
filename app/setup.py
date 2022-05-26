from setuptools import setup

setup(
    name = 'DemandSphere - test task',
    version = '1.0',
    author = 'Dawid Drążewski',
    license = 'MIT',
    package_dir = {'': 'src'},
    py_modules = ['database', 'logging_tools'],
    install_requires=['ijson==3.1.4', 'psycopg2==2.9.3']
)
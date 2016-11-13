
from setuputils import setup

version = "0.1"

setup(
    name='infinispan',
    version=version,
    description='Python client for Infinispan key-value store',
    long_description="",
    url='http://github.com/VaclavDedik/infinispan-py',
    author='Vaclav Dedik',
    author_email='vaclav.dedik@gmail.com',
    maintainer='Vaclav Dedik',
    maintainer_email='vaclav.dedik@gmail.com',
    keywords=['infinispan', 'key-value store'],
    license='MIT',
    packages=['infinispan'],
    install_requires=['future'],
    tests_require=[
        'pytest',
        'mock']
)

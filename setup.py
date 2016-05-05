from setuptools import setup
from ductworks import __version__, __author__, __author_email__

setup(
    name='ductworks',
    author=__author__,
    author_email=__author_email__,
    url="https://bitbucket.org/rpcope1/ductworks",
    version=__version__,
    description="A simple IPC library for less simple situations.",
    packages=['ductworks'],
    install_requires=[],
    tests_require=['assertpy'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: BSD License'
    ]
)

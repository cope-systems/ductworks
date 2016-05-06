from setuptools import setup
from textwrap import dedent
from ductworks import __version__, __author__, __author_email__

setup(
    name='ductworks',
    author=__author__,
    author_email=__author_email__,
    url="https://bitbucket.org/rpcope1/ductworks",
    version=__version__,
    description="A simple IPC library for less simple situations.",
    long_description=dedent("""
    Ductworks is an IPC library for Python, similar to the Pipe object from the multiprocessing library. Like Pipe,
    ductworks exposes a MessageDuct API with very simple send and receive semantics for end users (no worrying about
    streams, each send call corresponds to one recv call), while easily allowing for more flexible usage like having
    a communication channel between a parent python process and a python process in a child subprocess (separated by
    both fork(2) and exec(2)). As well ductworks allows users to freely choose their serialization library (and uses
    JSON by default) and uses regular BSD sockets; this makes connecting up programs in different languages (or even on
    different systems) relatively straightforward. Ductworks aims to make it simple to get socket pairs, when inheriting
    a anonymous socket pair is difficult or outright infeasible, and to make IPC easier and less stressful.
    """),
    packages=['ductworks'],
    install_requires=[],
    tests_require=['assertpy'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking',
        'License :: OSI Approved :: BSD License'
    ]
)

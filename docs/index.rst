.. Ductworks documentation master file, created by
   sphinx-quickstart on Sun Jun 19 17:54:39 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Ductworks for Python Documentation
==================================

Welcome to the Python Ductworks documentation.
Ductworks is an IPC library for Python, similar to the Pipe object from 
the multiprocessing library. Like Pipe, ductworks exposes a MessageDuct API 
with very simple send and receive semantics for end users (no worrying about 
streams, each send call corresponds to one recv call), while easily allowing 
for more flexible usage like having a communication channel between a parent 
python process and a python process in a child subprocess (separated by both 
fork(2) and exec(2)). As well ductworks allows users to freely choose their 
serialization library (and uses JSON by default) and uses regular BSD sockets; 
this makes connecting up programs in different languages (or even on different 
systems) relatively straightforward. Ductworks aims to make it simple to get 
socket pairs, when inheriting a anonymous socket pair is difficult or outright 
infeasible, and to make IPC easier and less stressful.

API Documentation
-----------------

* :ref:`message_duct_docs`
* :ref:`base_duct_docs`


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`


# Bayou Re-implementation

Authors: Lance Goodridge, David Gilhooley

Reimplementation of Bayou for 518 projects.

# David 

* Write client update function
* Write client resolve function
* Framework for randon update/resolves

# Lance 

* Get Logical clock working
* Write O vector
* Write Log structure

# Done

* Get MySQL working
* Get RPC working

# TODO

* Tuple Storage 
* Update Function
    * Dependency Checking 
* Server Logs -- All Write operations, sorted by Timestamp + commited or not
* Timestamp vectors
    * Logical Clock Simulation
* O Vector -- for each server, its the timestamp of latest Write discarded from log
* Anti-Entropy


# Bayou Re-implementation

Authors: Lance Goodridge, David Gilhooley

Reimplementation of Bayou for COS 518 project.

# David

* Write a checkEqual function for servers
* Simplify testing framework
  - Needs a create(name string) function
  - Needs easier way to send command
* Client CheckRoom and ClaimRoom methods
* Get persistent storage working
    * https://blog.golang.org/gobs-of-data
    * Commit/Tenative log
    * IsPrimary
    * CommitIndex
    * UndoLog
    * Ommited
    * Error Log

# Lance

* Unit test Bayou Server (all except anti-entropy)

# Done

* Get MySQL working
* Get RPC working
* Get Logical clock working
* Write server Read RPC handler
* Write server Write RPC handler
* Write client update function
* Write client resolve function
* Framework for random update/resolves

# TODO

* Anti-Entropy
* Investigate custom Test RPC class
* Set up testing infrastructure
* Perform tests


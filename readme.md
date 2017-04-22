# Bayou Re-implementation

Authors: Lance Goodridge, David Gilhooley

Reimplementation of Bayou for COS 518 project.

# David

* Write a checkEqual function for servers
* Simplify testing framework
  - Needs a create(name string) function
  - Needs easier way to send command
* Client CheckRoom and ClaimRoom methods

# Lance

* Server Start and Kill methods
* Change server RPCs to use strings instead

# Done

* Get MySQL working
* Get RPC working
* Get Logical clock working
* Write server AntiEntropy RPC Handler
* Write server Read RPC handler
* Write server Write RPC handler
* Write client update function
* Write client resolve function
* Implement server persistence

# TODO

* Unit tests
* Investigate custom RPC class
* Replication tests


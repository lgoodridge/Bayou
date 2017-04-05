# Bayou Re-implementation

Authors: Lance Goodridge, David Gilhooley

Reimplementation of Bayou for COS 518 project.

# David

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

* Implement Server Write and Rollback

# Done

* Get MySQL working
* Get RPC working
* Get Logical clock working
* Write server Read RPC handler
* Write client update function
* Write client resolve function
* Framework for random update/resolves

# TODO

* Write Function
    * Updating vector clocks
    * Updating logs
    * Applying to dataase
* Checkpointing
* Anti-Entropy
    * Log Rollbacking


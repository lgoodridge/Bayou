# Bayou Re-implementation

Authors: Lance Goodridge, David Gilhooley

Reimplementation of Bayou for COS 518 project.

# David

* Get persistant storage working
  * https://blog.golang.org/gobs-of-data
  * Commit/Tenative log
  * IsPrimary
  * CommitIndex
  * UndoLog
  * Ommited
  * Error Log

# Lance

* Figure out server persistence
* Write Bayou Read and Write RPC handlers

# Done

* Get MySQL working
* Get RPC working
* Get Logical clock working
* Write client update function
* Write client resolve function
* Framework for random update/resolves

# TODO

* Update Function
    * Dependency Checking
    * Merging
* Checkpointing
* Anti-Entropy
    * Log Rollbacking


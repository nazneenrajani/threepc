October 9 2013
CS 380D Distributed Computing : Project 1 Implement Three Phase Commit
Number of slip days used: 2

Team Members:
--------------
Nazneen Rajani nnr259
Harsh Pareek hhp266


Test cases handled:
-------------------
We handled the following test cases in our implementation:
1.No failures
2. Participant failure
3. Coordinator failure after partial precommit
4. Coordinator failure after precommit
5. Coordinator failure after partial commit
6. Cascading coordinator failure
7. Future coordinator failure
8. Total failure (2 variants)
9. Death after n messages from process p
10. Multiple failures

We also have a generic function which generalizes these cases. It takes an array of failure points for each process as its parameter.

Implementation details:
-----------------------
Our implementation of 3PC is contained in the files Participant.java, PlayList.java and Controller.java. We also modified NetController.java and Config.java.
NetController: sendMsg method also sends the sender's procNum. The received method return List<List<String>>
PlayList: Simple class using hashtables
Controller: This is the starting point for our program and has procNum=0 and does not participate in 3PC. It spawns n-1 other processes and sends a message invoking 3PC over the socket. This file also includes different test cases implemented as method.
Participant: This is the starting point for each of the n-1 processes and is called by the Controller with the following arguments:
*procNum(1 to n-1)
*failurePoint: (optional argument) Several important points in the code have labels assigned. Participant fails at the point where label==failurePoint. Permitted labels are described in possibleFailurePoints.txt
*deathAfterN: (optional argument) n for the deathAfter case in test cases, ignored if ==-1
*deathAfterP: (optional argument) p for the deathAfter case in test cases, ignored if ==-1

The Controller's job is to spawn processes and invoke 3PC, thereafter the Participants execute the 3PC algorithm as described in the paper.

Currently the votes of processes are hard-coded in the castVote(..) method for participants and invoke_3PC() for coordinator. They and can be modified in the respective functions.

How to run:
------------
* Set numProcesses in makeconfig.py. 
* Run "python makeconfig.py > config.properties"
* Set variable confPath in Participant.java and Controller.java
* Set file paths in config.properties.:
 - binPath = full path to bin/
 - logPath = full path to folder where you want the logs to appear
* execute Controller and uncomment desired test case


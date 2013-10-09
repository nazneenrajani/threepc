package ut.distcomp.framework;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

public class Participant {
	final static long timeOut = 5000L;
	static int id;
	static int lastCoordinator;
	static PlayList playList;
	static Config conf;
	static NetController nc;  
	static FileHandler fh;
	static List<List<String>> participant_recvdMsg;
	static BufferedWriter dtlog;
	static BufferedReader dtlogReader = null;
	static List<List<String>> bufferedMessages;
	static Integer[] UP;
	static String myState;
	static String myVote;
	static String finalDecision;
	
	static String failurePoint;
	/*static String confPath = "/home/nazneen/workspace/threepc/config.properties"; //TODO read these from config file
	static String binPath = "/home/nazneen/workspace/threepc/dc_project/bin/";
	static String logPath = "/home/nazneen/logs/";*/
	/*static String confPath = "C:/Users/Harsh/Documents/GitHub/threepc/config.properties"; //TODO read these from config file
	static String binPath = "C:/Users/Harsh/Documents/GitHub/threepc/dc_project/bin/";
	static String logPath = "C:/Users/Harsh/Desktop/logs/";*/
	static String confPath = "/home/harshp/code/threepc/config.properties"; //TODO read these from config file
	static String binPath = "/home/harshp/code/threepc/dc_project/bin/";
	static String logPath = "/home/harshp/logs/";
	static long delay = 10;

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
		bufferedMessages = new ArrayList<List<String>>();
		id=Integer.parseInt(args[0]);
		if(args.length>1)
			failurePoint = args[1];
		else
			failurePoint="";
		lastCoordinator = 1;
		myVote = "";
		finalDecision="";
		conf = new Config(confPath);
		conf.procNum=id;
		nc = new NetController(conf);
		UP = new Integer[conf.numProcesses];
		Boolean isRecoveryMode = false;
		File file = new File(logPath+"participant_"+id+".DTlog");
		FileWriter fw;
		if (!file.exists()){ 
			file.createNewFile();
			fw = new FileWriter(file.getAbsoluteFile());
		}
		else{
			isRecoveryMode = true;
			fw = new FileWriter(file.getAbsoluteFile(),true);
		}
		dtlog = new BufferedWriter(fw);
		dtlogReader = new BufferedReader(new FileReader(file));
		// This block configure the logger with handler and formatter
		System.setErr(new PrintStream(new FileOutputStream(logPath +"participant_"+id+".err")));
		if(!isRecoveryMode)
			fh = new FileHandler(logPath+"participant_"+id+".log", false);
		else{
			fh = new FileHandler(logPath+"participant_"+id+".log", true);
		}
		conf.logger.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();
		fh.setFormatter(formatter);
		log("Process "+id + " initialized"); 
		for(int j = 0;j<UP.length;j++)
			UP[j]=1;
		myState = null;
		//isRecoveryMode=true;
		if(!isRecoveryMode){
			myState = "UNCERTAIN";
			log("Started");
			DTLogWrite("START");
			//Boolean shutdownFlag=true;
			while(true){
				Thread.sleep(delay);
				participant_recvdMsg = nc.getReceivedMsgs();
				if(!participant_recvdMsg.isEmpty()){
					String[] message = null;
					for(List<String> command: participant_recvdMsg){
						message = command.get(1).split("##");
						switch(message[0]){
						case "INVOKE_3PC":
							log("Received "+command+" from Controller");
							Boolean finalDecision = invokeThreePC(message[1],message[2],message[3]);
							if(finalDecision)
								nc.sendMsg(0,"COMMIT");
							else
								nc.sendMsg(0,"ABORT");
							return;
						case "VOTE_REQ":
							DTLogWrite("VOTE_REQ");
							log("Received VOTE_REQ");
							Boolean myVote = castVote(message[1],message[2],message[3]);
							failHere("AFTER_VOTE");
							if(!myVote){
								myState = "ABORTED";
								DTLogWrite("ABORT");
								log("ABORT");
								abort();
							}
							else
								waitForDecision();
							sendFinalDecision(!myVote);	
							return;
						default:
							conf.logger.severe("invalid msg received " + message[0]);
							return;
						}
					}
				}
			}
		}
		else{
			log("Entering Recovery mode");
			myState="";
			String[] s =  returnLastLog();
			String lastState = s[0];
			lastCoordinator = Integer.parseInt(s[1]);
			String[] upString = s[2].split(",");
			for(int k = 0; k < conf.numProcesses;k++)
				UP[k] = Integer.parseInt(upString[k]);
			switch(lastState){
			case "START":
				myState = "ABORTED";
				DTLogWrite("ABORT");
				log("Last state START: ABORTING");
				//abort();
				break;
			case "VOTE_REQ":
				myState = "ABORTED";
				DTLogWrite("ABORT");
				log("Last state VOTE_REQ: ABORTING");
				//abort();
				break;
			case "YES":
				myState = "UNCERTAIN";
				log("Last state YES: Running Termination protocol");
				//participantRecovery();
				break;
			case "NO":
				myState = "ABORTED";
				DTLogWrite("ABORT");
				log("Last state NO: ABORTING");
				//abort();
				break;
			case "PRECOMMIT":
				myState = "COMMITABLE";
				log("Last state PRECOMMIT: Running Termination protocol");
				//participantRecovery(); 
				break;
			case "COMMIT":
				myState = "COMMITTED";
				log("I had committed before crash");
				//commit();
				break;
			case "ABORT":
				myState = "ABORTED";
				log("I had aborted before crash");
				//abort();
				break;
			case "START_3PC":
				log("Last state START_3PC: ABORTING");
				myState = "ABORTED";
				break;
			default:
				conf.logger.severe("Bad state in DTLog");
				break;
			}
			participantRecovery();
		}
	}

	private static void failHere(String location) {
		//log("In Failing here;");
		if(failurePoint.equals(location)){
			log("Failed at "+location);
			nc.sendMsg(0, "FAILING");
			System.exit(1);
		}
	}

	private static void participantRecovery() throws InterruptedException {
		log(myState + " is my current state in paritcipant recovery");
		if(myState.equals("UNCERTAIN") || myState.equals("COMMITABLE"))
			broadcast("FINALDECISION_REQ");
		List<List<String>> recvMsg;
		Integer[] zombie = new Integer[conf.numProcesses];
		Integer[] lastRunningProcesses = new Integer[conf.numProcesses];
		for(int k=0;k<zombie.length;k++)
			zombie[k]=0;
		zombie[conf.procNum]=1;
		for(int k=0;k<lastRunningProcesses.length;k++)
			lastRunningProcesses[k]=UP[k];
		String upString = UP[0].toString();
		for(int i=1; i< UP.length;i++)
			upString=upString+","+UP[i];
		upString = "UP##"+upString;
		broadcast(upString);
		Boolean sendUPString = false;
		Boolean totalFailureDetected = false;
		Boolean finalDecisionReceived = false;
		while(true){
			Thread.sleep(delay);
			recvMsg= nc.getReceivedMsgs();
			if(!recvMsg.isEmpty()){
				for(List<String> s : recvMsg){
					String[] s1;
					if(s.get(1).startsWith("UP##")){
						s1 = s.get(1).split("##");
						log(Arrays.toString(s1));
						zombie[Integer.parseInt(s.get(0))] = 1;
						String[] receivedUP = s1[1].split(",");
						for(int i = 1;i<conf.numProcesses;i++){
							if(Integer.parseInt(receivedUP[i])==0) 
								lastRunningProcesses[i] = 0;
						}
						sendUPString = false;
						for(int j = 0;j< lastRunningProcesses.length;j++){
							if(lastRunningProcesses[j]!=Integer.parseInt(receivedUP[j]))
								sendUPString = true;
						}
						Boolean isSubset = true;
						for(int i=1;i<conf.numProcesses;i++){
							if(lastRunningProcesses[i]==1)
								if(zombie[i]!=1)
									isSubset = false;
						}
						if(isSubset){
							log("Total failure detected");
							log("Zombie = "+Arrays.toString(zombie));
							log("lastRunningProcesses = "+Arrays.toString(lastRunningProcesses));
							totalFailureDetected = true;
						}
					}else if(s.get(1).equals("COMMIT")){
						myState = "COMMITTED";
						DTLogWrite("COMMIT");
						commit();
						finalDecisionReceived = true;
					}else if(s.get(1).equals("ABORT")){
						myState = "ABORTED";
						DTLogWrite("ABORT");
						abort();
						finalDecisionReceived = true;
					}
					else if(s.get(1).equals("FINALDECISION_REQ")){
						if(myState.equals("COMMITTED"))
							nc.sendMsg(Integer.parseInt(s.get(0)), "COMMIT");
						else if (myState.equals("ABORTED"))
							nc.sendMsg(Integer.parseInt(s.get(0)), "ABORT");
					}
				}
			}
			if(sendUPString){
				String lastString = lastRunningProcesses[0].toString();
				for(int i=1; i< lastRunningProcesses.length;i++)
					lastString=lastString+","+lastRunningProcesses[i];
				lastString = "UP##"+lastString;
				broadcast(lastString);
			}
			if(totalFailureDetected || finalDecisionReceived)
				break;
		}

		if(totalFailureDetected){
			if(myState.equals("COMMITTED") || myState.equals("ABORTED"))
				conf.logger.severe("Should not have been in TOTAL FAILURE");
			for(int i =0;i<UP.length;i++)
				UP[i] = zombie[i];
			electionProtocol();
		}
		else{
			if(myState.equals("COMMITTED"))
				commit();
			else if(myState.equals("ABORTED"))
				abort();
			else
				conf.logger.severe("Invalid state at end of participant recovery");
		}
	}

	private static void commit() {
		// TODO Auto-generated method stub

	}

	private static void abort() {
		// TODO Auto-generated method stub

	}

	private static String[] returnLastLog() {
		File file = new File(logPath+"participant_"+id+".DTlog");
		BufferedReader lastlineReader = null;
		ArrayList<String> lines = null;
		try {
			lastlineReader = new BufferedReader(new FileReader(file));
			lines = new ArrayList<String>();

			while (lastlineReader.ready())
			{
				lines.add(lastlineReader.readLine());
			}
			lastlineReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if(lines.size()==0){
			conf.logger.severe("DTLOG is empty!");
			return new String[0];
		}else{
			//log("I think last lien in DTLog is "+lines.get(lines.size()-1));
			return lines.get(lines.size()-1).split("\t");
		}
	}

	private static void waitForDecision() throws InterruptedException {
		long start = System.currentTimeMillis();
		while(true){
			Thread.sleep(delay);
			if(System.currentTimeMillis()-start > timeOut){
				log("Timed out on Coordinator waiting for Decision. Running election protocol");
				UP[getCoordinator()]=0;
				electionProtocol();
				return;
			}
			List<List<String>> recMsg = nc.getReceivedMsgs();
			for(List<String> s : recMsg){
				if(!recMsg.isEmpty()){
					String msg = s.get(1);
					if(msg.equals("ABORT")){
						myState = "ABORTED";
						DTLogWrite("ABORT");
						log("ABORTING");
						return;
					}
					else if(msg.equals("PRECOMMIT")){
						DTLogWrite("PRECOMMIT");
						log("Received PRECOMMIT");
						myState = "COMMITABLE";
						failHere("BEFORE_COMMIT");
						nc.sendMsg(getCoordinator(), "ACK");
						long start1 = System.currentTimeMillis();
						while(true){
							Thread.sleep(delay);
							if(System.currentTimeMillis()-start1 > timeOut){
								log("Timed out on Coordinator after sending ACK. Running election protocol");
								UP[getCoordinator()]=0;
								log(Arrays.toString(UP));
								electionProtocol();
								return;
							}
							List<List<String>> recvMsg = nc.getReceivedMsgs();
							if(!recvMsg.isEmpty()){
								for(List<String> msg1: recvMsg){
									if(msg1.get(1).equals("COMMIT")){
										DTLogWrite("COMMIT");
										log("Committing");
										myState = "COMMITTED";
										return;
									}else
										bufferedMessages.add(msg1);
								}
							}
						}
					}
				}
			}
		}
	}

	private static Boolean castVote(String command, String param1, String param2) {
		switch(command){
		case "add":
			conf.logger.info("Received add command");
			break;
		case "delete":
			conf.logger.info("Received delete command");
			break;
		case "edit":
			conf.logger.info("Received edit command");
			break;
		}
		Boolean vote = true;
		if(conf.procNum==-1){
			DTLogWrite("NO");
			vote = false;
			nc.sendMsg(getCoordinator(), "NO");
			myVote = "NO";
		}
		else{
			log("Writing yes to file");
			DTLogWrite("YES");
			nc.sendMsg(getCoordinator(), "YES");
			myVote = "YES";
		}
		return vote;
	}
	void sendMessage(int process, String msg){
		nc.sendMsg(process, msg);
		conf.logger.info("Process "+id+" sent "+ msg +" to Process "+process);
	}

	List<List<String>> getReceivedMsgs(){
		List<List<String>> recvMsgs = nc.getReceivedMsgs();
		for(List<String> s:recvMsgs){
			conf.logger.info("Process "+id+" received " + s);
		}
		return recvMsgs;	
	}

	public static void shutdown() throws IOException {
		dtlog.close();
		nc.shutdown();		
	}

	public static boolean invokeThreePC(String message, String s1, String s2) throws InterruptedException{
		// Vote REQ
		log("Sending VOTE_REQ");
		broadcast("VOTE_REQ##"+message+"##"+s1+"##"+s2);
		DTLogWrite("START_3PC");
		DTLogWrite("YES");
		// Collect votes
		List<String> exp = new ArrayList<String>();
		exp.add("YES");
		exp.add("NO");
		String[] votes=collectResults(exp,"YES");	
		boolean isAbort = false;
		// Decide "PC" or Abort
		log(Arrays.toString(votes));
		for(String vote : votes){
			if(vote.equals("NO") || vote.equals("")){
				isAbort = true;
				log("Deciding ABORT");
				myState = "ABORTED";
				DTLogWrite("ABORT");
				Boolean[] recipients = new Boolean[conf.numProcesses];
				for(int i=0;i<conf.numProcesses;i++){
					if(votes[i].equals("YES"))
						recipients[i]=true;
					else
						recipients[i]=false;
				}
				multicast("ABORT", recipients);
				break;
			}
		}
		log("here");
		if(!isAbort){
			DTLogWrite("PRECOMMIT");
			log("Decided PRECOMMIT");
			broadcast("PRECOMMIT");
			myState = "COMMITABLE";
			failHere("COORDINATOR_AFTER_PRECOMMIT");
		}
		else{
			sendFinalDecision(true);
			return false;
		}
		//System.setErr(err);
		//may need to send abort else send rpecommti and wait for ack
		exp = new ArrayList<String>();
		exp.add("ACK");
		collectResults(exp,"ACK");
		DTLogWrite("COMMIT");
		myState = "COMMITTED";
		log("Decided COMMIT");
		Boolean[] recipients = new Boolean[conf.numProcesses];
		for(int i=0;i<conf.numProcesses;i++)
			if(i<conf.numProcesses/2)
				recipients[i]=true;
			else
				recipients[i]=false;
		multicast("COMMIT", recipients);
		failHere("COORDINATOR_PARTIAL_COMMIT");
		Boolean[] invrecipients = new Boolean[conf.numProcesses];
		for(int i=0;i<conf.numProcesses;i++)
				invrecipients[i]=!recipients[i];
		multicast("COMMIT", invrecipients);
		//broadcast("COMMIT");
		// Send "Commit"
		commit();
		sendFinalDecision(false);
		return true;
	}

	private static void multicast(String msg, Boolean[] recipients){
		for(int i = 1; i< recipients.length;i++){
			if(recipients[i] == true && i !=conf.procNum){
				nc.sendMsg(i, msg);
			}
		}
	}

	private static void sendFinalDecision(boolean isAbort) throws InterruptedException {
		if((myState.equals("ABORTED")))
			finalDecision = "ABORT";
		else if(myState.equals("COMMITTED"))
			finalDecision = "COMMIT";
		else
			conf.logger.severe("Reached sendFinalDecision without having decided! Final State "+myState);
		//TODO handle  URelected
		Long start = System.currentTimeMillis();
		while(System.currentTimeMillis() - start < timeOut){
			Thread.sleep(delay);
			bufferedMessages.addAll(nc.getReceivedMsgs());// TODO some people may still not have got final decision
			if(!bufferedMessages.isEmpty()){
				for(int k = 0; k < bufferedMessages.size();k++){
					List<String> s = bufferedMessages.get(k);
					if(s.get(1).equals("FINALDECISION_REQ")){
						if(isAbort)
							nc.sendMsg(Integer.parseInt(s.get(0)),"ABORT");
						else
							nc.sendMsg(Integer.parseInt(s.get(0)),"COMMIT");
						bufferedMessages.remove(bufferedMessages.indexOf(s));
					}
					else if(s.get(1).equals("UR_ELECTED")){
						nc.sendMsg(Integer.parseInt(s.get(0)),finalDecision);
					}
				}
			}
		}
		log("Timed out after waiting for FINALDECISION_REQ");
	}

	static void broadcast(String msg){
		for(int i = 1; i < conf.numProcesses;i++)
			if(i!=conf.procNum)
				nc.sendMsg(i, msg);
	}

	static String[] collectResults(List<String> expectedAnswers, String defaultAnswer) throws InterruptedException
	{	
		Boolean[] check = new Boolean[conf.numProcesses];
		for(int j = 0; j< check.length; j++)
			check[j]=false;
		check[conf.procNum] = true;
		check[0] = true;
		String[] votes = new String[conf.numProcesses];
		for(int i = 0; i < votes.length;i++)
			votes[i] = "";
		votes[0] = defaultAnswer;
		votes[conf.procNum] = defaultAnswer; //TODO decide your vote
		//log("Entered collectResuilts");
		long start = System.currentTimeMillis();
		while(System.currentTimeMillis()-start < timeOut){
			Thread.sleep(delay);
			List<List<String>> currVotes= nc.getReceivedMsgs();
			//log(currVotes.toString());
			for(int k = 0; k < currVotes.size();k++){
				List<String> s = currVotes.get(k);
				check[Integer.parseInt(s.get(0))] = true; 
				if(!expectedAnswers.contains(s.get(1))){
					bufferedMessages.add(s);
					log("Unexpected response received "+s);
				}	
				else{
					votes[Integer.parseInt(s.get(0))] = s.get(1);
					//log(votes.toString());
				}
			}
			Boolean checkall = true;
			for(Boolean t: check)
				checkall = checkall && t;
			//log(checkall.toString());
			Boolean isNull = false;
			for(String s: votes){
				if(s.equals(""))
					isNull = true;
			}

			if(!isNull && checkall)
				break;
		}

		for(int k =0; k < check.length;k++){
			if(!check[k]){
				UP[k] = 0;
				log("Timed out on "+k+" in CollectResults");
			}
		}

		return votes;
	}

	static void log(String msg){
		conf.logger.info(msg);
	}

	static void DTLogWrite(String msg){
		try {
			dtlog.write(msg+"\t"+getCoordinator()+"\t");
			for(int i=0;i<conf.numProcesses;i++)
				dtlog.write(UP[i]+",");
			dtlog.write("\n");
			dtlog.flush();
		} catch (IOException e) {
			conf.logger.severe(e.toString());
		}
	}
	static int getCoordinator(){
		return lastCoordinator;
	}

	static int electionProtocol() throws InterruptedException{
		int j=1;
		for(;j<UP.length;j++){
			if(UP[j]==1 && j == conf.procNum){
				log("I am Coordinator");
				lastCoordinator = j;
				coordinatorElectionProtocol();
				break;
			}
			else if(UP[j]==1){
				log("Elected coordinator is "+j);
				nc.sendMsg(j, "UR_ELECTED");
				lastCoordinator = j;
				participantElectionProtocol();
				break;
			}
		}
		return j;
	}

	private static void participantElectionProtocol() throws InterruptedException{
		long start = System.currentTimeMillis();
		Boolean waitingforStateReq = true;
		while(waitingforStateReq){
			Thread.sleep(delay);
			if(System.currentTimeMillis()-start > timeOut){
				log("Timed out on "+getCoordinator()+ " waiting for STATE_REQ");;
				UP[getCoordinator()]=0;
				electionProtocol();
				break;
			}
			List<List<String>> recMsg = nc.getReceivedMsgs();
			for(List<String> s : recMsg){
				if(s.get(1).equals("STATE_REQ")){
					if(getCoordinator()!=Integer.parseInt(s.get(0)))
					{
						log("Old coordinator was " + getCoordinator() + "Received STATE_REQ from "+ s.get(0)+" Coordinator has changed to "+ s.get(0));
						for(int i = 1; i < Integer.parseInt(s.get(0));i++)
							UP[i] = 0;
						//electionProtocol();
						lastCoordinator= Integer.parseInt(s.get(0));
						//return; //TODO continue, and then return
					}
					waitingforStateReq=false;
				}
				else if(s.get(1).equals("UR_ELECTED")){
					for(int i = 1; i < conf.procNum;i++)
						UP[i] = 0;
					//electionProtocol();
					lastCoordinator = conf.procNum;
					waitingforStateReq=false;
				}
				else{
					bufferedMessages.add(s);
					log("Unexpected response received "+s);
				}
			}
		}

		if(myVote.equals("") || myVote.equals("NO") || returnLastLog()[0].equals("ABORT"))
			myState = "ABORTED";
		else if(returnLastLog()[0].equals("COMMIT"))
			myState = "COMMITTED";
		else if(returnLastLog()[0].equals("PRECOMMIT"))
			myState = "COMMITABLE";
		else
			myState = "UNCERTAIN";
		log("Sending state " + myState + " to " + getCoordinator());
		nc.sendMsg(getCoordinator(), myState);
		start = System.currentTimeMillis();
		while(true){
			Thread.sleep(delay);
			if(System.currentTimeMillis()-start > 2*timeOut){ // 2* timeout because coordinator is waiting for others
				log("Timed out on "+getCoordinator()+" while waiting for decision");
				UP[getCoordinator()]=0;
				electionProtocol();
				break;
			}
			List<List<String>> recMsg = nc.getReceivedMsgs();
			if(!recMsg.isEmpty()){
				log(recMsg+"");
				for(List<String> s : recMsg){
					String msg = s.get(1);
					if(msg.equals("ABORT")){
						myState = "ABORTED";
						DTLogWrite("ABORT");
						log("ABORTING");
						return;
					}
					else if (msg.equals("COMMIT")){
						DTLogWrite("COMMIT");
						myState = "COMMITTED";
						log("COMMITING");
						return;
					}
					else if(msg.equals("PRECOMMIT")){
						DTLogWrite("PRECOMMIT");
						log("Received PRECOMMIT");
						myState = "COMMITABLE";
						nc.sendMsg(getCoordinator(), "ACK");
						long start1 = System.currentTimeMillis();
						while(true){
							Thread.sleep(delay);
							if(System.currentTimeMillis()-start1 > timeOut){
								log("Timed out on "+getCoordinator()+" while waiting for COMMIT");
								UP[getCoordinator()]=0;
								electionProtocol();
								break;
							}
							List<List<String>> recvMsg = nc.getReceivedMsgs();
							if(!recvMsg.isEmpty()){
								for(List<String> msg1: recvMsg){
									if(msg1.get(1).equals("COMMIT")){
										DTLogWrite("COMMIT");
										log("Committing");
										myState = "COMMITTED";
										return;
									}else
										bufferedMessages.add(msg1);
								}
							}
						}
					}
					else{
						bufferedMessages.add(s);
					}
				}
			}
		}
	}

	private static void coordinatorElectionProtocol() throws InterruptedException {
		broadcast("STATE_REQ");
		List<String> possibleStates = new ArrayList<String>();
		possibleStates.add("UNCERTAIN");
		possibleStates.add("COMMITABLE");
		possibleStates.add("ABORTED");
		possibleStates.add("COMMITTED");
		String[] states = collectResults(possibleStates, myState);
		Boolean isAbort=false;
		log("Received states of participants:"+Arrays.toString(states));

		failHere("ELECTED_COORDINATOR_FAIL_AFTER_STATE_REQ");
		if(Arrays.asList(states).contains("ABORTED")){
			log("Some process has already aborted");
			if(!returnLastLog()[0].equals("ABORT"))
				DTLogWrite("ABORT");
			broadcast("ABORT");
			myState ="ABORTED";
			isAbort=true;
			abort();
		}
		else if(Arrays.asList(states).contains("COMMITTED")){
			log("Some process has already comitted");
			if(!returnLastLog()[0].equals("COMMIT"))
				DTLogWrite("COMMIT");
			//while(true){
			broadcast("COMMIT");
			//	if(0==1) break;
			//}
			myState="COMMITTED";
			commit();
		}
		else {
			Boolean ifAllUncertain = true;
			for (String s: states){
				if(!s.equals("UNCERTAIN") && !s.equals(""))
					ifAllUncertain = false;
			}
			if(ifAllUncertain){
				log("All processes are either uncertain or non-participating");
				DTLogWrite("ABORT");
				broadcast("ABORT");
				myState="ABORTED";
				isAbort=true;
				abort();
			}
			else{
				log("Some processes were committable");
				Boolean isAnyUncertain = false;
				Boolean[] recipients = new Boolean[conf.numProcesses];
				for(int i=1;i<conf.numProcesses;i++)
					if(states[i].equals("UNCERTAIN")){
						recipients[i] = true;
						isAnyUncertain = true;
					}
					else
						recipients[i] = false;
				//DTLogWrite("PRECOMMIT"); // not in paper. maybe?
				if(isAnyUncertain){ 
					multicast("PRECOMMIT", recipients);
					List<String> exp = new ArrayList<String>();
					exp.add("ACK");
					collectResults(exp,"ACK");
				}

				myState="COMMITABLE";
				DTLogWrite("COMMIT");
				myState="COMMITTED";
				//while(true){
					broadcast("COMMIT");
			//		if(0==1) break;
			//	}
			}
		}
		sendFinalDecision(isAbort);
		//Completed final decision. Send to controller
		if(!isAbort)
			nc.sendMsg(0,"COMMIT");
		else
			nc.sendMsg(0,"ABORT");
	}

}

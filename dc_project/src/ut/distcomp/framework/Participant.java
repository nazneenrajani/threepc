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
	static String failurePoint;

	public static void main(String[] args) throws FileNotFoundException, IOException{
		bufferedMessages = new ArrayList<List<String>>();
		id=Integer.parseInt(args[0]);
		System.setErr(new PrintStream(new FileOutputStream("/home/nazneen/logs/participant_"+id+".err")));
		if(args.length>1)
			failurePoint = args[1];
		else
			failurePoint="";
		lastCoordinator = 1;
		myVote = "";
		conf = new Config("/home/nazneen/workspace/threepc/config.properties");
		conf.procNum=id;
		//conf.logger.info("Process "+id+" started");
		UP = new Integer[conf.numProcesses];
		Boolean isRecoveryMode = false;
		File file = new File("/home/nazneen/logs/participant_"+id+".DTlog");
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
		if (!isRecoveryMode) 
			fh = new FileHandler("/home/nazneen/logs/participant_"+id+".log",false);
		else{
			fh = new FileHandler("/home/nazneen/logs/participant_"+id+".log",true);
			//DTLogWrite("RECOVERY");
		}
		//Handler ch = new ConsoleHandler();
		//conf.logger.addHandler(ch);
		conf.logger.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();  
		fh.setFormatter(formatter); 
		log("Process "+id + " initialized"); 
		nc = new NetController(conf);
		for(int j = 0;j<UP.length;j++)
			UP[j]=1;
		myState = "UNCERTAIN";
		//isRecoveryMode=true;
		if(!isRecoveryMode){
			log("Started");
			DTLogWrite("START");
			Boolean shutdownFlag=true;
			while(shutdownFlag){
				participant_recvdMsg = nc.getReceivedMsgs();
				if(!participant_recvdMsg.isEmpty()){
					String[] message = null;
					for(List<String> command: participant_recvdMsg){
						message = command.get(1).split("##");
						switch(message[0]){
						case "INVOKE_3PC":
							//DTLogWrite("INVOKE_3PC");
							log("Received "+command+" from Controller");
							Boolean finalDecision = invokeThreePC(message[1],message[2],message[3]);
							if(finalDecision)
								nc.sendMsg(0,"COMMIT");
							else
								nc.sendMsg(0,"ABORT");
							break;
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
							break;
						case "SHUTDOWN":
							DTLogWrite("SHUTDOWN");
							shutdown();
							shutdownFlag=false;
							break;
						default:
							conf.logger.severe("invalid msg received " + message[0]);
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
				log("ABORT after recovery");
				//abort();
				break;
			case "VOTE_REQ":
				myState = "ABORTED";
				DTLogWrite("ABORT");
				log("ABORT after recovery");
				//abort();
				break;
			case "YES":
				myState = "UNCERTAIN";
				log("Running termination protocol as participant");
				//participantRecovery();
				break;
			case "NO":
				myState = "ABORTED";
				DTLogWrite("ABORT");
				log("ABORT after recovery");
				//abort();
				break;
			case "PRECOMMIT":
				myState = "COMMITABLE";
				log("Running termination protocol as participant");
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
			case "INVOKE_3PC":
				break;
			default:
				conf.logger.severe("Bad state in DTLog");
				break;
			}
			participantRecovery();
		}
	}

	private static void failHere(String location) {
		log("In Failing here;");
		if(failurePoint.equals(location)){
			log("Failed at "+location);
			System.exit(1);
		}
	}

	private static void participantRecovery() {
		log(myState);
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
			recvMsg= nc.getReceivedMsgs();
			if(!recvMsg.isEmpty()){
				for(List<String> s : recvMsg){
					String[] s1;
					if(s.get(1).startsWith("UP##")){
						s1 = s.get(1).split("##");
						log(Arrays.toString(s1));
						zombie[Integer.parseInt(s.get(0))] = 1;
						String[] t = s1[1].split(",");
						for(int i = 0;i<conf.numProcesses;i++){
							if(Integer.parseInt(t[i])==0) 
								lastRunningProcesses[i] = 0;
						}
						sendUPString = true;
						Boolean isSubset = true;
						for(int i=0;i<conf.numProcesses;i++){
							if(lastRunningProcesses[i]==1)
								if(zombie[i]!=1)
									isSubset = false;
						}
						if(isSubset){
							log("Total failure detected");
							log(Arrays.toString(zombie));
							log(Arrays.toString(lastRunningProcesses));
							totalFailureDetected = true;
						}
					}else if(s.get(1).equals("COMMIT")){
						DTLogWrite("COMMIT");
						commit();
						finalDecisionReceived = true;
					}else if(s.get(1).equals("ABORT")){
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
			if(sendUPString)
				broadcast(upString);
			if(totalFailureDetected || finalDecisionReceived)
				break;
		}

		if(totalFailureDetected){
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
		ArrayList<String> lines = new ArrayList<String>();
		try {
			while (dtlogReader.ready())
			{
				lines.add(dtlogReader.readLine());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}	
		return lines.get(lines.size()-1).split("\t");
	}

	private static void waitForDecision() {
		long start = System.currentTimeMillis();
		while(true){
			if(System.currentTimeMillis()-start > timeOut){
				UP[getCoordinator()]=0;
				electionProtocol();
				break;
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
							if(System.currentTimeMillis()-start1 > timeOut){
								UP[getCoordinator()]=0;
								electionProtocol();
								break;
							}
							List<List<String>> commitMsg = nc.getReceivedMsgs();
							if(!commitMsg.isEmpty()){
								DTLogWrite("COMMIT");
								log("Committing");
								myState = "COMMITTED";
								return;
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
		if(conf.procNum==3){
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

	public static boolean invokeThreePC(String message, String s1, String s2){
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
		log(votes.toString());
		for(String vote : votes){
			if(vote.equals("NO") || vote.equals("")){
				isAbort = true;
				log("Deciding ABORT");
				DTLogWrite("ABORT");
				Boolean[] recipients = new Boolean[conf.numProcesses];
				for(int i=0;i<conf.numProcesses;i++){
					if(votes[i].equals("YES"))
						recipients[i]=true;
					else
						recipients[i]=false;
				}
				log("Before ABORT");
				log(recipients.length+"");
				multicast("ABORT", recipients);
				log("After ABORT");
				myState = "ABORTED";
				break;
			}
		}
		log("here");
		if(!isAbort){
			DTLogWrite("PRECOMMIT");
			log("Decided PRECOMMIT");
			broadcast("PRECOMMIT");
			myState = "COMMITABLE";
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
		log("Decided COMMIT");
		broadcast("COMMIT");
		myState = "COMMITTED";
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

	private static void sendFinalDecision(boolean isAbort) {
		Long start = System.currentTimeMillis();
		while(System.currentTimeMillis() - start < timeOut){
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
				}
			}
		}
	}

	static void broadcast(String msg){
		for(int i = 1; i < conf.numProcesses;i++)
			if(i!=conf.procNum)
				nc.sendMsg(i, msg);
	}

	static String[] collectResults(List<String> expectedAnswers, String defaultAnswer)
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
			if(!check[k])
				UP[k] = 0;
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

	static int electionProtocol(){
		int j=1;
		for(;j<UP.length;j++){
			if(UP[j]==1 && j == conf.procNum){
				log("I am Coordinator");
				lastCoordinator = j;
				coordinatorElectionProtocol();
				break;
			}
			else if(UP[j]==1){
				nc.sendMsg(j, "UR_ELECTED");
				lastCoordinator = j;
				participantElectionProtocol();
				break;
			}
		}
		return j;
	}

	private static void participantElectionProtocol(){
		long start = System.currentTimeMillis();
		while(true){
			if(System.currentTimeMillis()-start > timeOut){
				UP[getCoordinator()]=0;
				electionProtocol();
				break;
			}
			List<List<String>> recMsg = nc.getReceivedMsgs();
			for(List<String> s : recMsg){
				if(s.get(1).equals("STATE_REQ")){
					for(int i = 1; i<=Integer.parseInt(s.get(0));i++)
						UP[i] = 0;
					electionProtocol();
					break; //TODO maybe return
				}
				else if(s.get(1).equals("UR_ELECTED")){
					for(int i = 1; i<conf.procNum;i++)
						UP[i] = 0;
					electionProtocol();
					break;
				}
				else{
					bufferedMessages.add(s);
					log("Unexpected response received "+s);
					//TODO return maybe
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
		nc.sendMsg(getCoordinator(), myState);
		start = System.currentTimeMillis();
		while(true){
			if(System.currentTimeMillis()-start > timeOut){
				UP[getCoordinator()]=0;
				electionProtocol();
				break;
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
					else if (msg.equals("COMMIT")){
						DTLogWrite("COMMIT");
					}
					else if(msg.equals("PRECOMMIT")){
						DTLogWrite("PRECOMMIT");
						log("Received PRECOMMIT");
						myState = "COMMITABLE";
						nc.sendMsg(getCoordinator(), "ACK");
						long start1 = System.currentTimeMillis();
						while(true){
							if(System.currentTimeMillis()-start1 > timeOut){
								UP[getCoordinator()]=0;
								electionProtocol();
								break;
							}
							List<List<String>> commitMsg = nc.getReceivedMsgs();
							if(!commitMsg.isEmpty()){
								DTLogWrite("COMMIT");
								log("Committing");
								myState = "COMMITTED";
								return;
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

	private static void coordinatorElectionProtocol() {
		broadcast("STATE_REQ");
		List<String> possibleStates = new ArrayList<String>();
		possibleStates.add("UNCERTAIN");
		possibleStates.add("COMMITABLE");
		possibleStates.add("ABORTED");
		possibleStates.add("COMMITTED");
		String[] states = collectResults(possibleStates, myState);
		Boolean isAbort=false;

		if(Arrays.asList(states).contains("ABORTED")){
			if(!returnLastLog()[0].equals("ABORT"))
				DTLogWrite("ABORT");
			broadcast("ABORT");
			myState ="ABORTED";
			isAbort=true;
			abort();
		}
		else if(Arrays.asList(states).contains("COMMITTED")){
			if(!returnLastLog()[0].equals("COMMIT"))
				DTLogWrite("COMMIT");
			broadcast("COMMIT");
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
				DTLogWrite("ABORT");
				broadcast("ABORT");
				myState="ABORTED";
				isAbort=true;
				abort();
			}
			else{
				DTLogWrite("PRECOMMIT");
				broadcast("PRECOMMIT");
				myState="COMMITABLE";
				Boolean[] recipients = new Boolean[conf.numProcesses];
				for(int i=1;i<conf.numProcesses;i++)
					if(states[i].equals("UNCERTAIN"))
						recipients[i] = true;
					else
						recipients[i] = false;
				List<String> exp = new ArrayList<String>();
				exp.add("ACK");
				collectResults(exp,"ACK");
				DTLogWrite("COMMIT");
				myState="COMMITTED";
				multicast("COMMIT", recipients);
			}
		}
		sendFinalDecision(isAbort);
	}

}

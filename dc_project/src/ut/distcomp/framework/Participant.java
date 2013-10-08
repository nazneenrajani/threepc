package ut.distcomp.framework;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.SimpleFormatter;

public class Participant {
	final static long timeOut = 2000L;
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

	public static void main(String[] args) throws FileNotFoundException, IOException{ 
		bufferedMessages = new ArrayList<List<String>>();
		id=Integer.parseInt(args[0]);
		lastCoordinator = 1;
		conf = new Config("/home/nazneen/workspace/threepc/config.properties");
		//conf.logger.info("Process "+id+" started");
		UP = new Integer[conf.numProcesses];
		Boolean isRecoveryMode = false;
		File file = new File("/home/nazneen/logs/participant_"+id+".DTlog");
		if (!file.exists()) 
			file.createNewFile();
		else
			isRecoveryMode = true;
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		dtlog = new BufferedWriter(fw);
		dtlogReader = new BufferedReader(new FileReader(file));
		// This block configure the logger with handler and formatter
		if (!isRecoveryMode) 
			fh = new FileHandler("/home/nazneen/logs/participant_"+id+".log",false);
		else{
			fh = new FileHandler("/home/nazneen/logs/participant_"+id+".log",true);
			DTLogWrite("Recovered from crash");
		}
		//Handler ch = new ConsoleHandler();
		//conf.logger.addHandler(ch);
		conf.logger.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();  
		fh.setFormatter(formatter); 
		log("Process "+id + " initialized"); 
		conf.procNum=id;
		nc = new NetController(conf);
		for(int j = 0;j<UP.length;j++)
			UP[j]=1;
		log("Started");
		DTLogWrite("START");
		myState = "UNCERTAIN";
		if(!isRecoveryMode){
			Boolean shutdownFlag=true;
			while(shutdownFlag){
				//List<String> msg_history = null;
				participant_recvdMsg = nc.getReceivedMsgs();
				if(!participant_recvdMsg.isEmpty()){
					String[] message = null;
					for(List<String> command: participant_recvdMsg){
						message = command.get(1).split("##");
						//msg_history.add(message[0]);
						//TODO Check message order
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
			myState = "FAILED";
			String[] s =  returnLastLog();
			String lastState = s[0];
			lastCoordinator = Integer.parseInt(s[1]);
			switch(lastState){
			case "START":
				DTLogWrite("ABORT");
				log("ABORT after recovery");
				abort();
				break;
			case "VOTE_REQ":
				DTLogWrite("ABORT");
				log("ABORT after recovery");
				abort();
				break;
			case "YES":
				log("Running termination protocol as participant");
				participantRecovery(); // TODO maybe change for coordinator failure case
				break;
			case "NO":
				DTLogWrite("ABORT");
				log("ABORT after recovery");
				abort();
				break;
			case "PRECOMMIT":
				log("Running termination protocol as participant");
				participantRecovery(); // TODO maybe change for coordinator failure case
				break;
			case "COMMIT":
				log("I had committed before crash");
				commit();
				break;
			case "ABORT":
				log("I had aborted before crash");
				abort();
				break;
			case "INVOKE_3PC":
				break;
			default:
				conf.logger.severe("Bad state in DTLog");
				break;
			}
		}
	}

	private static void participantRecovery() {
		broadcast("FINALDECISION_REQ");
		
		while(true){
			//TODO timeout > all timeouts
			List<List<String>> recMsg = nc.getReceivedMsgs();
			if(!recMsg.isEmpty()){
				String decision = recMsg.get(0).get(1);
				if(decision.equals("COMMIT")){
					DTLogWrite("COMMIT");
					commit();
				}	
				else{
					DTLogWrite("ABORT");
					abort();
				}
				break;
			}
		}
	}

	private static void commit() {
		// TODO Auto-generated method stub

	}

	private static void abort() {
		// TODO Auto-generated method stub

	}

	private static String[] returnLastLog() {	 
		String sCurrentLine = ""; 
		try {
			while ((sCurrentLine = dtlogReader.readLine()) != null) {
				;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return sCurrentLine.split("\t");
	}

	private static void waitForDecision() throws IOException {
		
		long start = System.currentTimeMillis();
		while(true){
			if(System.currentTimeMillis()-start < timeOut){
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
						nc.sendMsg(getCoordinator(), "ACK");
						long start1 = System.currentTimeMillis();
						while(true){
							if(System.currentTimeMillis()-start1 < timeOut){
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
		//if(conf.procNum==3)
		//nc.sendMsg(getCoordinator(), "NO");
		//else
		Boolean vote = true;
		DTLogWrite("YES");
		nc.sendMsg(getCoordinator(), "YES");
		return vote;
	}
	/**/	
	/*
	public Participant(int id,Config conf){		
		this.id = id;
		this.viewNumber = 0;
		this.conf = conf;
		conf.procNum = id;
		this.playList= new PlayList();
		nc = new NetController(conf);
	}
	 */
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
				for(int i=0;i<conf.numProcesses;i++)
					if(votes[i].equals("YES"))
						recipients[i]=true;
				multicast("ABORT", recipients);
				myState = "ABORTED";
				break;
			}
		}
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
			if(recipients[i] == true && i !=conf.procNum)
				nc.sendMsg(i, msg);
		}
	}
	private static void sendFinalDecision(boolean isAbort) {
		bufferedMessages.addAll(nc.getReceivedMsgs());// TODO some people may still not have got final decision
		//TODO timeout
		//log(currVotes.toString());
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
		//TODO participant election protocol recovery
		int j=1;
		for(;j<UP.length;j++){
			if(UP[j]==1 && j == conf.procNum){
				log("I am Coordinator");
				coordinatorRecovery();
				break;
			}
			else if(UP[j]==1){
				nc.sendMsg(j, "UR_ELECTED");
				break;
			}
		}
		return j;
	}

	private static void coordinatorRecovery() {
		broadcast("STATE_REQUEST");
		List<String> possibleStates = new ArrayList<String>();
		possibleStates.add("UNCERTAIN");
		possibleStates.add("COMMITABLE");
		possibleStates.add("ABORTED");
		possibleStates.add("COMMITTED");
		String[] states = collectResults(possibleStates, myState);
		
		if(Arrays.asList(states).contains("ABORTED")){
			if(!returnLastLog()[0].equals("ABORT"))
				DTLogWrite("ABORT");
			broadcast("ABORT");
			abort();
		}
		else if(Arrays.asList(states).contains("COMMITTED")){
			if(!returnLastLog()[0].equals("COMMIT"))
				DTLogWrite("COMMIT");
			broadcast("COMMIT");
			commit();
		}
		else {
			Boolean isUncertain = true;
			for (String s: states){
				if(!s.equals("UNCERTAIN"))
					isUncertain = false;
			}
			if(isUncertain){
				DTLogWrite("ABORT");
				broadcast("ABORT");
				abort();
			}
			else{
				DTLogWrite("PRECOMMIT");
				broadcast("PRECOMMIT");
				Boolean[] recipients = new Boolean[conf.numProcesses];
				for(int i=1;i<conf.numProcesses;i++)
					if(states[i].equals("UNCERTAIN"))
						recipients[i] = true;
				List<String> exp = new ArrayList<String>();
				exp.add("ACK");
				collectResults(exp,"ACK");
				DTLogWrite("COMMIT");
				multicast("COMMIT", recipients);
			}
		}
	}
}

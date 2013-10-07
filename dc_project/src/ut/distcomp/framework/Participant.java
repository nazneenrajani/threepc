package ut.distcomp.framework;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;


public class Participant {
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
		if(!isRecoveryMode){
			Boolean shutdownFlag=true;
			while(shutdownFlag){
				List<String> msg_history = null;
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
		//TODO timeout, election
		while(true){
			List<List<String>> recMsg = nc.getReceivedMsgs();
			for(List<String> s : recMsg){
				if(!recMsg.isEmpty()){
					String msg = s.get(1);
					if(msg.equals("ABORT")){
						DTLogWrite("ABORT");
						log("ABORTING");
						return;
					}
					else if(msg.equals("PRECOMMIT")){
						DTLogWrite("PRECOMMIT");
						log("Received PRECOMMIT");
						nc.sendMsg(getCoordinator(), "ACK");
						while(true){
							//TODO timeout and election
							List<List<String>> commitMsg = nc.getReceivedMsgs();
							if(!commitMsg.isEmpty()){
								DTLogWrite("COMMIT");
								log("Committing");
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
		List<String> votes=collectResults(exp);	

		// Decide "PC" or Abort
		log(votes.toString());
		boolean isAbort = false;
		for(String vote : votes){
			if(vote.equals("NO")){
				isAbort = true;
				log("Deciding ABORT");
				DTLogWrite("ABORT");
				broadcast("ABORT"); //TODO only to yes ppl
			}
		}
		if(!isAbort){
			DTLogWrite("PRECOMMIT");
			log("Decided PRECOMMIT");
			broadcast("PRECOMMIT");
		}
		else{
			sendFinalDecision(true);
			return false;
		}
		//System.setErr(err);
		//may need to send abort else send rpecommti and wait for ack
		exp = new ArrayList<String>();
		exp.add("ACK");
		List<String> acks=collectResults(exp);
		DTLogWrite("COMMIT");
		log("Decided COMMIT");
		broadcast("COMMIT");
		// Send "Commit"
		commit();
		sendFinalDecision(false);
		return true;
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

	static List<String> collectResults(List<String> expectedAnswers)
	{	
		Boolean[] check = new Boolean[conf.numProcesses];
		for(int j = 0; j< check.length; j++)
			check[j]=false;
		check[conf.procNum] = true;
		check[0] = true;
		List<String> votes = new ArrayList<String>();
		//log("Entered collectResuilts");
		while(true){ // TODO TINEOUT
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
					votes.add(s.get(1));
					//log(votes.toString());
				}
			}
			Boolean checkall = true;
			for(Boolean t: check)
				checkall = checkall && t;
			//log(checkall.toString());
			if(votes.size()==conf.numProcesses-2 && checkall)
				break;
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

	int electionProtocol(){
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

	private void coordinatorRecovery() {
		broadcast("STATE_REQUEST");
		List<String> possibleStates = new ArrayList<String>();
		possibleStates.add("START");
		possibleStates.add("UNCERTAIN");
		possibleStates.add("COMMITABLE");
		possibleStates.add("ABORTED");
		possibleStates.add("COMMITTED");
		List<String> states = collectResults(possibleStates);
		//TODO timeout skip
		//TODO add my state in collectresults
		if(states.contains("ABORTED")){
			if(!returnLastLog()[0].equals("ABORT"))
				DTLogWrite("ABORT");
			broadcast("ABORT");
			abort();
		}
		else if(states.contains("COMMITTED")){
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
				//TODO multicast only to ppl uncertain
				List<String> exp = new ArrayList<String>();
				exp.add("ACK");
				List<String> acks=collectResults(exp);
				DTLogWrite("COMMIT");
				broadcast("COMMIT");
			}
		}
	}
}

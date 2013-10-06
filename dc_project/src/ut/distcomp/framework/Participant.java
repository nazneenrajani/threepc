package ut.distcomp.framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import sun.awt.geom.Curve;

public class Participant {
	static int id;
	int viewNumber;
	PlayList playList;
	static Config conf;
	static NetController nc;  
	static FileHandler fh;
	static List<List<String>> participant_recvdMsg;

	public static void main(String[] args) throws FileNotFoundException, IOException{ 
		id=Integer.parseInt(args[0]);
		conf = new Config("/home/nazneen/workspace/threepc/config.properties");
		conf.logger.info("Process "+id+" started");
		try {  
			// This block configure the logger with handler and formatter  
			fh = new FileHandler("/home/nazneen/logs/participant_"+id+".log");  
			conf.logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();  
			fh.setFormatter(formatter); 
			conf.logger.info("Process "+id + " initialized");
		} catch (SecurityException e) {  
			e.printStackTrace();  
		} catch (IOException e) {  
			e.printStackTrace();  
		}  
		conf.procNum=id;
		nc = new NetController(conf);
		Boolean flag=true;
		while(flag){
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
						conf.logger.info("Received "+command+" from Controller");
						Boolean finalDecision = invokeThreePC(message[1],message[2],message[3]);
						if(finalDecision)
							nc.sendMsg(0,"COMMIT");
						else
							nc.sendMsg(0,"ABORT");
						break;
					case "VOTE_REQ":
						log("Received VOTE_REQ");
						castVote(message[1],message[2],message[3]);
						waitForDecision();
						break;
					case "SHUTDOWN":
						nc.shutdown();
						flag=false;
						break;
					default:
						conf.logger.severe("invalid msg received " + message[0]);
					}
				}
			}
		}
	}

	private static void waitForDecision() {
		while(true){
			List<List<String>> recMsg = nc.getReceivedMsgs();
			for(List<String> s : recMsg){
				if(!recMsg.isEmpty()){
					String msg = s.get(1);
					if(msg.equals("ABORT")){
						log("ABORTING");
						return;
					}
					else if(msg.equals("PRECOMMIT")){
						log("Received PRECOMMIT");
						nc.sendMsg(1, "ACK");
						while(true){
							List<List<String>> commitMsg = nc.getReceivedMsgs();
							if(!commitMsg.isEmpty()){
								log("Committing");
								return;
							}
						}
					}

				}

			}
		}
	}

	private static void castVote(String command, String param1, String param2) {
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
		if(conf.procNum==3)
			nc.sendMsg(1, "NO");
		else
			nc.sendMsg(1, "YES");
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

	public void shutdown() {
		nc.shutdown();		
	}

	public static boolean invokeThreePC(String message, String s1, String s2){
		// Vote REQ
		log("Sending VOTE_REQ");
		broadcast("VOTE_REQ##"+message+"##"+s1+"##"+s2);

		// Collect votes
		List<String> exp = new ArrayList<String>();
		exp.add("YES");
		exp.add("NO");
		List<String> votes=collectResults(exp);	

		// Decide "PC" or Abort
		boolean isAbort = false;
		for(String vote : votes){
			if(vote.equals("NO")){
				isAbort = true;
				log("Deciding ABORT");
				broadcast("ABORT");
				return false;
			}			
		}
		if(!isAbort){
			broadcast("PRECOMMIT");
			log("Decided PRECOMMIT");
		}

		//may need to send abort else send rpecommti and wait for ack
		exp = new ArrayList<String>();
		exp.add("ACK");
		List<String> acks=collectResults(exp);
		broadcast("COMMIT");
		log("Decided COMMIT");
		// Send "Commit" 

		return true;
	}

	static void broadcast(String msg){
		for(int i = 1; i < conf.numProcesses;i++)
			if(i!=conf.procNum)
				nc.sendMsg(i, msg);
	}

	static List<String> collectResults(List<String> expectedAnswers)
	{
		Boolean[] check = new Boolean[conf.numProcesses-1];
		check[conf.procNum] = true;
		List<String> votes = new ArrayList<String>();
		while(true){
			List<List<String>> currVotes= nc.getReceivedMsgs();
			for(List<String> s:currVotes){
				check[Integer.parseInt(s.get(0))] = true;
				if(!expectedAnswers.contains(s.get(1))){
					conf.logger.severe("Invalid response received "+s);
				}	
				else
					votes.add(s.get(1));
			}
			Boolean checkall = true;
			for(Boolean t: check)
				checkall = checkall && t;
			if(votes.size()==conf.numProcesses-2 && checkall)
				break;
		}
		return votes;
	}

	static void log(String msg){
		conf.logger.info(msg);
	}
}

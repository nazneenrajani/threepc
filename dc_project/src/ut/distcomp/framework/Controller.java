package ut.distcomp.framework;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Controller {
	static ArrayList<Process> listParticipant;
	static Config host_conf = null;
	static NetController host_nc;
	static int currentCoordinator = 1;
	static String confPath = "/home/nazneen/workspace/threepc/config.properties"; //TODO read these from config file
	static String binPath = "/home/nazneen/workspace/threepc/dc_project/bin/";
	static String logPath = "/home/nazneen/logs/";
	/*static String confPath = "C:/Users/Harsh/Documents/GitHub/threepc/config.properties"; //TODO read these from config file
	static String binPath = "C:/Users/Harsh/Documents/GitHub/threepc/dc_project/bin/";
	static String logPath = "C:/Users/Harsh/Desktop/logs/";*/
//	static String confPath = "/home/harshp/code/threepc/config.properties"; //TODO read these from config file
//	static String binPath = "/home/harshp/code/threepc/dc_project/bin/";
//	static String logPath = "/home/harshp/logs/";
	static long delay = 10;
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
		host_conf = new Config(confPath);
		host_nc = new NetController(host_conf);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				for(Process p: listParticipant)
					p.destroy();
				host_nc.shutdown();
				host_conf.logger.info("shutting down");
			}
		});
		host_conf.procNum = 0;

		//noFailuresTest();
		
		//participantFailure(); // Participant is not able to use port again on coming back if we use messages. Need to exit gracefully

		//cascadingCoordinatorFailure(); // some process sends STATE_REQ to another, but other puts it in buffer and forgets about it. Then thinks the first is dead

		//futureCoordinatorFailure();

		//partialPrecommitFailure();

		//partialCommitFailure(); // Works okay except 2 is not expecting to be made coordinator. 3 updates its UP incorrectly

		totalFailure(); 
		
		//multipleFailure(); // TODO

		host_conf.logger.info("Shutting down");
		System.exit(0);
	}
	
	private static void totalFailure() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		for(int i=2;i<host_conf.numProcesses;i++)
			errorlocations[i] = "AFTER_VOTE";
		genericFailure(errorlocations);
	}
	
	private static void totalFailure2() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		for(int i=2;i<host_conf.numProcesses;i++)
			errorlocations[i] = "AFTER_VOTE";
		genericFailure(errorlocations);
	}
	
	private static void partialCommitFailure() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_PARTIAL_COMMIT";
		genericFailure(errorlocations);
	}
	
	private static void partialPrecommitFailure() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		genericFailure(errorlocations);
	}
	
	private static void futureCoordinatorFailure() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		errorlocations[2]="AFTER_VOTE";
		genericFailure(errorlocations);
	}
	
	private static void cascadingCoordinatorFailure() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		errorlocations[2]="ELECTED_COORDINATOR_FAIL_AFTER_STATE_REQ";
		errorlocations[3]="ELECTED_COORDINATOR_FAIL_AFTER_STATE_REQ";
		genericFailure(errorlocations);
	}
	
	private static void participantFailure() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[3]="AFTER_VOTE";
		errorlocations[4]="AFTER_VOTE";
		genericFailure(errorlocations);
	}

	private static void noFailuresTest() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		genericFailure(errorlocations);
	}
	
	private static void deleteDTLog(){
		for(int i = 1; i < host_conf.numProcesses;i++){
			File file = new File(logPath +"participant_"+i+".DTlog");
			File file1 = new File(logPath +"participant_"+i+".log");
			File file2 = new File(logPath +"participant_"+i+".log.lck");
			file.delete();
			file1.delete();
			file2.delete();
		}
	}
	
	private static void genericFailure(String[] errorlocations) throws IOException, InterruptedException {	
		int participants = host_conf.numProcesses;
		List<List<String>> recvdMsg;

		deleteDTLog();
		listParticipant= new ArrayList<Process>();	
		for(int i = 1; i <participants; i++){
			Process p = null;
			p= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant "+ i + " "+errorlocations[i]);
			listParticipant.add(i-1,p);
		}
		Thread.sleep(1000);
		//TODO wait for processes to stasrt
		int c= findCoordinator();
		//Iterator it = recvdMsg.iterator();
		String command = "add";
		String s1 = "a";
		String s2 = "a.song";
		host_nc.sendMsg(c, "INVOKE_3PC##"+command+"##"+s1+"##"+s2);
		Long start = System.currentTimeMillis();
		Boolean receivedResponse = false;
		while(true){
			Thread.sleep(delay);
			//System.out.println("In loop");
			for(Process p:listParticipant){
				try{
					System.out.println(p.exitValue());
					System.out.println((listParticipant.indexOf(p)+1) + " Dead");
					int index = listParticipant.indexOf(p);
					host_conf.logger.info("Starting Process "+(listParticipant.indexOf(p)+1)+" Again");
					p= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant " + (listParticipant.indexOf(p)+1));
					listParticipant.set(index, p);
					//TODO restart p
				}catch(IllegalThreadStateException e){
					//System.out.println(listParticipant.indexOf(p)+1+" Still running");
					continue;
				}
			}
			//Thread.sleep(1000);
			recvdMsg = host_nc.getReceivedMsgs();
			//System.out.println(recvdMsg);
			if(!recvdMsg.isEmpty())
			{
				for(List<String> s: recvdMsg){
				if(s.get(1).equals("COMMIT")){
					host_conf.logger.info("Committed");
					receivedResponse = true;
				}
				else if(s.get(1).equals("ABORT")){
					host_conf.logger.info("Command Aborted");
				/* }else if(s.get(1).equals("FAILING")){
					if(s.get(0).equals("1"))
						currentCoordinator = 2;
					host_conf.logger.info("Starting Process "+s.get(0)+" Again");
					Process p= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant " + s.get(0));
					listParticipant.set(Integer.parseInt(s.get(0))-1, p); */
				} 
				}
				if(receivedResponse)
					break;
				//if(System.currentTimeMillis() - start > 10000L)
				//	break;
			}		
		}
	}

	private static int findCoordinator(){
		return currentCoordinator;
	}
}

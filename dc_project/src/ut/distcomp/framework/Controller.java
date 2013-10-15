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
	static String confPath = "/home/nazneen/workspace/threepc/config.properties";
	static String binPath;
	static String logPath;
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
		binPath = host_conf.binPath;
		logPath = host_conf.logPath;
		//noFailuresTest();
		//testcase2();
		//testcase3();
		//testcase4();
		//testcase5();

		//change to config1 and rm *
		//testcase6();
		//testcase7();
		//testcase8();

		//testcase9();
		//testcase10();
		testcase11();

		//participantFailure(); 
		//cascadingCoordinatorFailure();
		//futureCoordinatorFailure();
		//partialPrecommitFailure();
		//precommitFailure();
		//partialCommitFailure();
		//totalFailure();
		//totalFailure2(); 
		//multipleTotalFailure();
		//multipleTotalFailure2();
		//deathAfterFailure();
		host_conf.logger.info("Shutting down");
		System.exit(0);
	}

	private static void testcase2() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[3]="VOTE_REQ";
		genericFailure(errorlocations);		
	}

	private static void testcase3() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[2]="BEFORE_COMMIT";
		errorlocations[3]="BEFORE_COMMIT";
		genericFailure(errorlocations);		
	}

	private static void testcase4() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_PARTIAL_PRECOMMIT";
		genericFailure(errorlocations);		
	}

	private static void testcase5() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_PARTIAL_COMMIT";
		genericFailure(errorlocations);		
	}
	private static void testcase6() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_PARTIAL_PRECOMMIT";
		errorlocations[2]="ELECTED_COORDINATOR_BEFORE_PRECOMMIT";
		genericFailure(errorlocations);		
	}
	private static void testcase7() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_PARTIAL_PRECOMMIT";
		errorlocations[2]="ELECTED_COORDINATOR_PARTIAL_PRECOMMIT";
		genericFailure(errorlocations);	
	}
	private static void testcase8() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_PARTIAL_PRECOMMIT";
		errorlocations[2]="ELECTED_COORDINATOR_BEFORE_COMMIT";
		genericFailure(errorlocations);		
	}
	private static void testcase9() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		errorlocations[2]="PRECOMMIT";
		errorlocations[3]="COMMIT";
		genericFailure(errorlocations);		
	}
	private static void testcase10() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COMMIT";
		errorlocations[2]="ELECTED_COORDINATOR_FAIL_AFTER_STATE_REQ";
		errorlocations[3]="ELECTED_COORDINATOR_FAIL_AFTER_STATE_REQ";
		genericFailure(errorlocations);		
	}
	private static void testcase11() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		errorlocations[2]="ELECTED_COORDINATOR_BEFORE_COMMIT";
		extraCreditFailure(errorlocations);		
	}

	private static void extraCreditFailure(String[] errorlocations) throws IOException, InterruptedException {
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
		int c= findCoordinator();
		String command = "add";
		String s1 = "a";
		String s2 = "a.song";
		host_nc.sendMsg(c, "INVOKE_3PC##"+command+"##"+s1+"##"+s2);
		Long start = System.currentTimeMillis();
		Boolean receivedResponse = false;
		long dead_3=0L;
		while(true){
			Thread.sleep(delay);
			for(Process p:listParticipant){
				if(listParticipant.indexOf(p)!=1 ){
					try{
						//Thread.sleep(100000);
						System.out.println(p.exitValue());
						System.out.println((listParticipant.indexOf(p)+1) + " Dead");
						int index = listParticipant.indexOf(p);
						//if(index==0||index==2){
						host_conf.logger.info("Starting Process "+(listParticipant.indexOf(p)+1)+" Again");
						p= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant " + (listParticipant.indexOf(p)+1));
						listParticipant.set(index, p);
						//}
					}catch(IllegalThreadStateException e){
						continue;
					}
				}
				else{
					//if(receivedResponse){
					if(System.currentTimeMillis()-dead_3>5000L){
						try{
							System.out.println(p.exitValue());
							System.out.println((listParticipant.indexOf(p)+1) + " Dead");
							int index = listParticipant.indexOf(p);
							host_conf.logger.info("Starting Process "+(listParticipant.indexOf(p)+1)+" Again");
							p= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant " + (listParticipant.indexOf(p)+1) + " EXTRA_CREDIT");
							listParticipant.set(index, p);
						}catch(IllegalThreadStateException e){
							continue;
						}
					}
				}
			}
			recvdMsg = host_nc.getReceivedMsgs();
			if(!recvdMsg.isEmpty())
			{
				for(List<String> s: recvdMsg){
					if(s.get(1).equals("COMMIT")){
						host_conf.logger.info("Committed");
						receivedResponse = true;
					}
					else if(s.get(1).equals("ABORT")){
						host_conf.logger.info("Command Aborted");
						receivedResponse = true;
					} 
					else if(s.get(1).equals("FAILING") && s.get(0).equals("2")){
						host_conf.logger.info("Killing 3");
						listParticipant.get(2).destroy();
						dead_3 = System.currentTimeMillis();
					}
				}
			}		
		}

	}

	private static void deathAfterFailure() throws InterruptedException, IOException {
		int[] n = new int[host_conf.numProcesses];
		int[] p = new int[host_conf.numProcesses];
		for(int i=0;i<host_conf.numProcesses; i++){
			n[i]=-1;
			p[i]=-1;
		}
		n[5]=2;
		p[5]=1;
		deathAfter(n,p);
	}

	private static void deathAfter(int[] n, int[] p) throws InterruptedException, IOException {
		int participants = host_conf.numProcesses;
		List<List<String>> recvdMsg;

		deleteDTLog();
		listParticipant= new ArrayList<Process>();	
		for(int i = 1; i <participants; i++){
			Process proc = null;
			proc= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant "+ i + " NOFAIL "+n[i]+" "+p[i]);
			listParticipant.add(i-1,proc);
		}
		Thread.sleep(1000);
		int c= findCoordinator();
		String command = "add";
		String s1 = "a";
		String s2 = "a.song";
		host_nc.sendMsg(c, "INVOKE_3PC##"+command+"##"+s1+"##"+s2);
		Long start = System.currentTimeMillis();
		Boolean receivedResponse = false;
		while(true){
			Thread.sleep(delay);
			Process process = null;
			for(Process proc:listParticipant){
				try{
					System.out.println(proc.exitValue());
					System.out.println((listParticipant.indexOf(proc)+1) + " Dead");
					int index = listParticipant.indexOf(proc);
					host_conf.logger.info("Starting Process "+(listParticipant.indexOf(proc)+1)+" Again");
					proc= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant " + (listParticipant.indexOf(proc)+1));
					listParticipant.set(index, proc);
				}catch(IllegalThreadStateException e){
					continue;
				}
			}
			recvdMsg = host_nc.getReceivedMsgs();
			if(!recvdMsg.isEmpty())
			{
				for(List<String> s: recvdMsg){
					if(s.get(1).equals("COMMIT")){
						host_conf.logger.info("Committed");
						receivedResponse = true;
					}
					else if(s.get(1).equals("ABORT")){
						host_conf.logger.info("Command Aborted");
						receivedResponse = true;
					} 
				}
				if(receivedResponse)
					break;
			}		
		}

	}

	private static void partialPrecommitFailure() throws IOException, InterruptedException {
		String[] errorlocations =  new String[host_conf.numProcesses];
		for(int i=1;i<host_conf.numProcesses;i++)
			errorlocations[i] = "";
		errorlocations[1]="COORDINATOR_PARTIAL_PRECOMMIT";
		genericFailure(errorlocations);
	}

	private static void multipleTotalFailure() throws IOException, InterruptedException{
		String[] errorlocations =  new String[host_conf.numProcesses];
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		errorlocations[2]="AFTER_VOTE";
		errorlocations[3]="AFTER_VOTE";
		errorlocations[4]="AFTER_VOTE";
		for(int i=5;i<host_conf.numProcesses;i++)
			errorlocations[i] = "AFTER_VOTE";
		String[] errorlocations2 =  new String[host_conf.numProcesses];
		errorlocations2[1]="COORDINATOR_PARTIAL_COMMIT";
		errorlocations2[2]="RECOVERY_PARTICIPANT_FAIL_AFTER_PRECOMMIT";
		errorlocations2[3]="RECOVERY_PARTICIPANT_FAIL_AFTER_STATE_REQ";
		errorlocations2[4]="RECOVERY_PARTICIPANT_FAIL_AFTER_PRECOMMIT";
		for(int i=5;i<host_conf.numProcesses;i++)
			errorlocations2[i] = "AFTER_VOTE";
		multipleFailureGeneric(errorlocations, errorlocations2);	
	}

	private static void multipleTotalFailure2() throws IOException, InterruptedException{
		String[] errorlocations =  new String[host_conf.numProcesses];
		errorlocations[1]="COORDINATOR_AFTER_PRECOMMIT";
		errorlocations[2]="START";
		errorlocations[3]="VOTE_REQ";
		errorlocations[4]="AFTER_VOTE";
		for(int i=5;i<host_conf.numProcesses;i++)
			errorlocations[i] = "AFTER_VOTE";
		String[] errorlocations2 =  new String[host_conf.numProcesses];
		errorlocations2[1]="COORDINATOR_PARTIAL_COMMIT";
		errorlocations2[2]="RECOVERY_PARTICIPANT_FAIL_AFTER_PRECOMMIT";
		errorlocations2[3]="RECOVERY_PARTICIPANT_FAIL_AFTER_STATE_REQ";
		errorlocations2[4]="RECOVERY_PARTICIPANT_FAIL_AFTER_PRECOMMIT";
		for(int i=5;i<host_conf.numProcesses;i++)
			errorlocations2[i] = "AFTER_VOTE";
		multipleFailureGeneric(errorlocations, errorlocations2);	
	}


	private static void multipleFailureGeneric(String[] errorlocations,String[] errorlocations2) throws IOException, InterruptedException {
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
		int c= findCoordinator();
		String command = "add";
		String s1 = "a";
		String s2 = "a.song";
		host_nc.sendMsg(c, "INVOKE_3PC##"+command+"##"+s1+"##"+s2);
		Long start = System.currentTimeMillis();
		Boolean receivedResponse = false;
		Boolean[] failedOnce = new Boolean[host_conf.numProcesses];
		for(int i=0;i<host_conf.numProcesses; i++)
			failedOnce[i] = false;
		while(true){
			Thread.sleep(delay);
			for(Process p:listParticipant){
				try{
					System.out.println(p.exitValue());
					System.out.println((listParticipant.indexOf(p)+1) + " Dead");
					int index = listParticipant.indexOf(p);
					host_conf.logger.info("Starting Process "+(listParticipant.indexOf(p)+1)+" Again");
					if(failedOnce[index+1])
						p= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant " + (listParticipant.indexOf(p)+1));
					else{
						p= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant " + (listParticipant.indexOf(p)+1)+" "+ errorlocations2[index+1]);
						failedOnce[index+1] = true;
					}
					listParticipant.set(index, p);
				}catch(IllegalThreadStateException e){
					continue;
				}
			}
			recvdMsg = host_nc.getReceivedMsgs();
			if(!recvdMsg.isEmpty())
			{
				for(List<String> s: recvdMsg){
					if(s.get(1).equals("COMMIT")){
						host_conf.logger.info("Committed");
						receivedResponse = true;
					}
					else if(s.get(1).equals("ABORT")){
						host_conf.logger.info("Command Aborted");
						receivedResponse = true;
					} 
				}
				if(receivedResponse)
					break;
			}		
		}
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
		errorlocations[2]="VOTE_REQ";
		errorlocations[3]="START";
		errorlocations[4]="NO";
		for(int i=5;i<host_conf.numProcesses;i++)
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

	private static void precommitFailure() throws IOException, InterruptedException {
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
		int c= findCoordinator();
		String command = "add";
		String s1 = "a";
		String s2 = "a.song";
		host_nc.sendMsg(c, "INVOKE_3PC##"+command+"##"+s1+"##"+s2);
		Long start = System.currentTimeMillis();
		Boolean receivedResponse = false;
		while(true){
			Thread.sleep(delay);
			for(Process p:listParticipant){
				try{
					System.out.println(p.exitValue());
					Thread.sleep(100000);
					System.out.println((listParticipant.indexOf(p)+1) + " Dead");
					int index = listParticipant.indexOf(p);
					//if(index==1 || index==2){
					host_conf.logger.info("Starting Process "+(listParticipant.indexOf(p)+1)+" Again");
					p= Runtime.getRuntime().exec("java -cp "+ binPath +" ut.distcomp.framework.Participant " + (listParticipant.indexOf(p)+1));
					listParticipant.set(index, p);
					//}
				}catch(IllegalThreadStateException e){
					continue;
				}
			}
			recvdMsg = host_nc.getReceivedMsgs();
			if(!recvdMsg.isEmpty())
			{
				for(List<String> s: recvdMsg){
					if(s.get(1).equals("COMMIT")){
						host_conf.logger.info("Committed");
						receivedResponse = true;
					}
					else if(s.get(1).equals("ABORT")){
						host_conf.logger.info("Command Aborted");
						receivedResponse = true;
					} 
				}
				if(receivedResponse)
					break;
			}		
		}
	}

	private static int findCoordinator(){
		return currentCoordinator;
	}
}

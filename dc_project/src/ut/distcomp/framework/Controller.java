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
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
		host_conf = new Config("/home/nazneen/workspace/threepc/config.properties");
		host_nc = new NetController(host_conf);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				for(int i=1;i<host_conf.numProcesses;i++)
					host_nc.sendMsg(i, "SHUTDOWN");
				for(Process p: listParticipant)
					p.destroy();
				host_nc.shutdown();
				host_conf.logger.info("shutting down");
			}
		});
		host_conf.procNum = 0;

		//noFailuresTest();

		participantFailure();

		coordinatorFailure();

		cascadingCoordinatorFailure();

		futureCoordinatorFailure();

		partialPrecommiteFailure();

		partialCommitFailure();

		totalFailure();

		//deleteDTLog();
		host_conf.logger.info("Shutting down");
		System.exit(0);
	}

	private static void totalFailure() {
		// TODO Auto-generated method stub

	}

	private static void partialCommitFailure() {
		// TODO Auto-generated method stub

	}

	private static void partialPrecommiteFailure() {
		// TODO Auto-generated method stub

	}

	private static void futureCoordinatorFailure() {
		// TODO Auto-generated method stub

	}

	private static void cascadingCoordinatorFailure() {
		// TODO Auto-generated method stub

	}

	private static void coordinatorFailure() {
		// TODO Auto-generated method stub

	}

	private static void participantFailure() throws IOException, InterruptedException {
		int participants = host_conf.numProcesses;
		List<List<String>> recvdMsg;

		deleteDTLog();
		listParticipant= new ArrayList<Process>();	
		for(int i = 1; i <participants; i++){
			Process p = null;
			if(i==3)
				p= Runtime.getRuntime().exec("java -cp /home/nazneen/workspace/threepc/dc_project/bin/ ut.distcomp.framework.Participant "+ i + " AFTER_VOTE");
			else if(i==4)
				p= Runtime.getRuntime().exec("java -cp /home/nazneen/workspace/threepc/dc_project/bin/ ut.distcomp.framework.Participant "+ i + " AFTER_VOTE");
			else
				p= Runtime.getRuntime().exec("java -cp /home/nazneen/workspace/threepc/dc_project/bin/ ut.distcomp.framework.Participant "+ i);

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
		while(true){
			//System.out.println("In loop");
			for(Process p:listParticipant){
				try{
					System.out.println(p.exitValue());
					System.out.println((listParticipant.indexOf(p)+1) + " Dead");
					int index = listParticipant.indexOf(p);
					host_conf.logger.info("Starting Process "+(listParticipant.indexOf(p)+1)+" Again");
					p= Runtime.getRuntime().exec("java -cp /home/nazneen/workspace/threepc/dc_project/bin/ ut.distcomp.framework.Participant " + (listParticipant.indexOf(p)+1));
					listParticipant.set(index, p);
					//TODO restart p
				}catch(IllegalThreadStateException e){
					//System.out.println(listParticipant.indexOf(p)+1+" Still running");
					continue;
				}
			}
			Thread.sleep(1000);
			recvdMsg = host_nc.getReceivedMsgs();
			//System.out.println(recvdMsg);
			if(!recvdMsg.isEmpty() && System.currentTimeMillis() - start > 10000L)
				break;
		}
		if(recvdMsg.get(0).get(1).equals("COMMIT")){
			host_conf.logger.info("Committed");
		}
		else if(recvdMsg.get(0).get(1).equals("ABORT")){
			host_conf.logger.info("Command Aborted");
		}
	}

	private static void noFailuresTest() throws IOException, InterruptedException{
		int participants = host_conf.numProcesses;
		List<List<String>> recvdMsg;

		deleteDTLog();
		listParticipant= new ArrayList<Process>();	
		for(int i = 1; i <participants; i++){
			Process p = Runtime.getRuntime().exec("java -cp /home/nazneen/workspace/threepc/dc_project/bin/ ut.distcomp.framework.Participant "+i);
			/*BufferedReader err=new BufferedReader(new InputStreamReader(p.getErrorStream()));
					String s;
					while ((s=err.readLine())!=null)
				    {
				           System.out.println(s);
				    }*/
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
		while(true){
			for(Process p:listParticipant){
				try{
					p.exitValue();
					//System.out.println(listParticipant.indexOf(p)+1 + " Dead");
					//p.
					//TODO restart p
				}catch(IllegalThreadStateException e){
					//System.out.println(listParticipant.indexOf(p)+1+" Still running");
					continue;
				}
			}

			recvdMsg = host_nc.getReceivedMsgs();
			//System.out.println(recvdMsg);
			if(!recvdMsg.isEmpty())
				break;
		}
		if(recvdMsg.get(0).get(1).equals("COMMIT")){
			host_conf.logger.info("Committed");
		}
		else if(recvdMsg.get(0).get(1).equals("ABORT")){
			host_conf.logger.info("Command Aborted");
		}

		for(Process p:listParticipant)
			p.destroy();
	}

	private static void deleteDTLog(){
		for(int i = 1; i < host_conf.numProcesses;i++){
			File file = new File("/home/nazneen/logs/participant_"+i+".DTlog");
			file.delete();
		}
	}
	private static int findCoordinator(){
		return currentCoordinator;
	}
}

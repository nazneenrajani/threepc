package ut.distcomp.framework;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Controller {
	static ArrayList<Process> listParticipant;
	static Config host_conf = null;
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
		host_conf = new Config("/home/nazneen/workspace/threepc/config.properties");
		final NetController host_nc = new NetController(host_conf);
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
			recvdMsg = host_nc.getReceivedMsgs();		
			if(!recvdMsg.isEmpty())
				break;
		}
		//System.out.println("Outside break");
		//System.out.println(recvdMsg.get(0));
		if(recvdMsg.get(0).get(1).equals("COMMIT")){
			host_conf.logger.info("Committed");
		}
		else if(recvdMsg.get(0).get(1).equals("ABORT")){
			host_conf.logger.info("Command Aborted");
		}
		//deleteDTLog();
		//listParticipant.get(c).invokeThreePC(1, "a", "http://a.song");
		/*
		for(Participant p:listParticipant){
			p.shutdown();
		}
		*/	
		host_conf.logger.info("Shutting down");
		System.exit(0);
	}
	
	private static void deleteDTLog(){
		for(int i = 1; i < host_conf.numProcesses;i++){
			File file = new File("/home/nazneen/logs/participant_"+i+".DTlog");
			file.delete();
		}
	}
	private static int findCoordinator(){
		//TODO find first active process
		return 1;
	}
}

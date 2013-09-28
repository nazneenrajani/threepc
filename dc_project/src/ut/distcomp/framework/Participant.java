package ut.distcomp.framework;

public class Participant {
	int id;
	int viewNumber;
	PlayList playList;
	final Config conf;
	
	public Participant(int id,Config conf){
		this.id = id;
		this.viewNumber = 0;
		this.conf = conf;
		this.playList = new PlayList();
	}
	
	void sendMessage(){
		
	}
}

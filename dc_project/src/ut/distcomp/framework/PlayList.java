package ut.distcomp.framework;

import java.util.Hashtable;
import java.util.logging.Logger;

public class PlayList {
	public static Hashtable<String, String> playlist = new Hashtable<String, String>();
	void add(String songName, String URL){
		if(!playlist.containsKey(songName))
			playlist.put(songName,URL);
//TODO: put else
	}
	
	void delete(String songName){
		if(playlist.containsKey(songName))
			playlist.remove(songName);
	//TODO	else
	}
	
	void editName(String songName, String newsongName){
		if(playlist.containsKey(songName)){
			playlist.put(newsongName, playlist.get(songName));
			playlist.remove(songName);
		}
	//TODO	else		
	}
	
	void editUrl(String songName, String newURL){
		if(playlist.containsKey(songName))
			playlist.put(songName, newURL);
		//TODO else
	}
}

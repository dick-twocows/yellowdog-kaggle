package org.twocows.yellowdog.kaggle;

public class KaggleFactory {
	
	public final static String LINE_TOPIC = "kaggle";

	public final static String BOOTSTRAP_SERVERS = "localhost:9092";
	
	public final static long LINES_EOF_ID = -1;
	
	public final static KaggleLine LINES_EOF = new KaggleLine(LINES_EOF_ID, new String[0]); 
}

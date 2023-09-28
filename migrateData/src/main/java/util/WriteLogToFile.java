package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteLogToFile {
	
	public static void log_Write(String log) {
		BufferedWriter writer = null ;
		try {		
			File f = new File("LOG.txt");
			
			writer = new BufferedWriter(new FileWriter("LOG.txt", true));
			writer.append(log);
	
		} catch (IOException e1) { 
			e1.printStackTrace();
			log_Write(e1.toString());
		} finally {		
			try {
				writer.close();
			} catch (IOException e11) {				
				e11.printStackTrace();
				log_Write(e11.toString());
			}
		}
	}

}

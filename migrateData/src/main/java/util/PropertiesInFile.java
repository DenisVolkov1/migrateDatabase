package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PropertiesInFile {
	
	public static Properties getRunProperties() {
		File f = new File("run.properties");
		
		if(!f.exists() && !f.isDirectory()) createNewRunProperties();
		
		InputStream  input  = null;
		Properties prop = null;
	    try {
	    	input  = new FileInputStream("run.properties");
	        prop = new Properties();
	        prop.load(input);
	        	//System.out.println("getRunProperties "+prop);
	    } catch (IOException io) {
	    	LOg.ERROR(io);
	    } finally {
			try {
			  input.close();
			} catch (IOException e1) {
				LOg.ERROR(e1);
			}
		}
		return prop;	
	}
	public static void createNewRunProperties() {
		OutputStream output = null;
	    try {
	    	output = new FileOutputStream("run.properties");
	        Properties prop = new Properties();
	
	        // Properties database MSSQLSERVER
	        prop.setProperty("ip_mssql", "<ip_mssql>");
	        prop.setProperty("port_mssql", "<port_mssql>");
	        prop.setProperty("databaseName_mssql", "<databaseName_mssql>");
	        prop.setProperty("schema_mssql", "<schema_mssql>");
	        prop.setProperty("user_mssql", "<user_mssql>");
	        prop.setProperty("password_mssql", "<password_mssql>");
	        // Properties database PostgresSQL
	        prop.setProperty("ip_postgre", "<ip_postgre>");
	        prop.setProperty("port_postgre", "<port_postgre>");
	        prop.setProperty("databaseName_postgre", "<databaseName_postgre>");
	        prop.setProperty("schema_postgre", "<schema_postgre>");
	        prop.setProperty("user_postgre", "<user_postgre>");
	        prop.setProperty("password_postgre", "<password_postgre>");

	        // save properties to project root folder
	        prop.store(output, null);
	
	    } catch (IOException io) {
	    	LOg.ERROR(io);
	    } finally {
			try {
				if(output!=null) output.close();
			} catch (IOException e1) {				
				LOg.ERROR(e1);
			}
		}
	}

}

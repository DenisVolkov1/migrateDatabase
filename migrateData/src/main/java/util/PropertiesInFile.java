package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PropertiesInFile {
	
	public static Properties getRunProperties() throws FileNotFoundException {
		File f = new File("run.properties");
		
		if(!f.exists() && !f.isDirectory()) {
			//createNewRunProperties();
			String errFile="";
			errFile=
					"\n------------------------------------------------------\n"+
					" Создйте файл 'run.properties' в той же директории. \n"+
					" Установите все необходимые свойства. \n"+
					"------------------------------------------------------\n"+
			        "#// Properties database MSSQLSERVER \n"+
			        "ip_mssql <ip_mssql> \n"+
			        "port_mssql <port_mssql> \n"+
			        "databaseName_mssql <databaseName_mssql> \n"+
			        "schema_mssql <schema_mssql> \n"+
			        "user_mssql <user_mssql> \n"+
			        "password_mssql <password_mssql> \n"+
	
			        "#// Properties database PostgresSQL  \n"+
			        "ip_postgre <ip_postgre> \n"+
			        "port_postgre <port_postgre> \n"+
			        "databaseName_postgre <databaseName_postgre> \n"+
			        "schema_postgre <schema_postgre> \n"+
			        "user_postgre <user_postgre> \n"+
			        "password_postgre <password_postgre> \n"+
			        
			        "#// Extra props \n"+
			        "is_use_all_schemas 0 \n"+
			        "is_use_multithread 0 \n"+
			        "\n------------------------------------------------------\n";
			
			throw new FileNotFoundException(errFile);
		}
		
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
	        // Extra props
	        prop.setProperty("is_use_all_schemas", "0");
	        prop.setProperty("is_use_multithread", "0");

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

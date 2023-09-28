package connection;

import java.util.Properties;

import util.PropertiesInFile;

public class PropMSSQLConnection extends PropConnection {


	public PropMSSQLConnection(String ip, String port, String databaseName, String schema, String user,
			String password) {
		super(ip, port, databaseName, schema, user, password);
		// TODO Auto-generated constructor stub
	}
	
	public PropMSSQLConnection(Properties prop) {
		super();
		String ip = prop.getProperty("ip_mssql");
		String port = prop.getProperty("port_mssql");
		String databaseName = prop.getProperty("databaseName_mssql");
		String schema = prop.getProperty("schema_mssql");
		String user = prop.getProperty("user_mssql");
		String password = prop.getProperty("password_mssql");
		
		this.ip = ip;
		this.port = port;
		this.databaseName = databaseName;
		this.schema = schema;
		this.user = user;
		this.password = password;
		checkNullEmpty();
	}
	

	@Override
	public String getStringConnection() {
		super.checkNullEmpty();
		return "jdbc:sqlserver://"+ip+":"+port+";encrypt=false;databaseName="+databaseName+";currentSchema="+schema+";user="+user+";password="+password;
	}
	


}

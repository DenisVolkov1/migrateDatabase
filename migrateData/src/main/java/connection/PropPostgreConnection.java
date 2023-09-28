package connection;

import java.util.Properties;

public class PropPostgreConnection extends PropConnection {

	public PropPostgreConnection(String ip, String port, String databaseName, String schema, String user,
			String password) {
		super(ip, port, databaseName, schema, user, password);
		// TODO Auto-generated constructor stub
	}
	
	public PropPostgreConnection(Properties prop) {
		super();
		String ip = prop.getProperty("ip_postgre");
		String port = prop.getProperty("port_postgre");
		String databaseName = prop.getProperty("databaseName_postgre");
		String schema = prop.getProperty("schema_postgre");
		String user = prop.getProperty("user_postgre");
		String password = prop.getProperty("password_postgre");
		
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
		return "jdbc:postgresql://"+ip+":"+port+"/"+databaseName+"?currentSchema="+schema+"&user="+user+"&password="+password;
	}

}

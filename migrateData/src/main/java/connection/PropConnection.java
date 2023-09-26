package connection;

public abstract class PropConnection {
	////"jdbc:sqlserver://localhost:1434;encrypt=false;databaseName=SCPRD;currentSchema=wmwhse1;user=sa;password=sql";
	
	protected String ip ;
	protected String port;
	protected String databaseName;
	protected String schema;
	protected String user;
	protected String password;
	
	public PropConnection(String ip, String port, String databaseName, String schema, String user, String password) {
		super();
		this.ip = ip;
		this.port = port;
		this.databaseName = databaseName;
		this.schema = schema;
		this.user = user;
		this.password = password;
	}
	public abstract String getStringConnection();
	
//	public String getStringConnectionToMSSql() {
//		checkNullEmpty();
//		return "jdbc:sqlserver://"+ip+":"+port+";encrypt=false;databaseName="+databaseName+";currentSchema="+schema+";user="+user+";password="+password;	
//	}
//	public String getStringConnectionToPostgreSQL() {
//		checkNullEmpty();
//		return "jdbc:postgresql://"+ip+":"+port+"/"+databaseName+"?currentSchema="+schema+"&user="+user+"&password="+password;	
//	}
	
	protected void checkNullEmpty() {
		if(ip == null || port == null || databaseName == null || schema == null || user == null || password == null) {
			throw new RuntimeException("<NULL> is not acceptable!");
		}
		if(ip.isEmpty() || port.isEmpty() || databaseName.isEmpty() || schema.isEmpty() || user.isEmpty() || password.isEmpty()) {
			throw new RuntimeException("<EMPTY> is not acceptable!");
		}
	}

	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	
	
}

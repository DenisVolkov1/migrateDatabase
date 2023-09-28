package connection;

import java.util.Properties;

import util.PropertiesInFile;

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
		checkNullEmpty();
	}
	
	public PropConnection() {}

	protected abstract String getStringConnection();
	
	protected void checkNullEmpty() {
		if(ip == null || port == null || databaseName == null || schema == null || user == null || password == null) {
			throw new RuntimeException("<NULL> is not acceptable!");
		}
		if(ip.isEmpty() || port.isEmpty() || databaseName.isEmpty() || schema.isEmpty() || user.isEmpty() || password.isEmpty()) {
			throw new RuntimeException("<EMPTY> is not acceptable!");
		}
	}
	
	public String getIp() {
		checkNullEmpty();
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
		checkNullEmpty();
	}
	public String getPort() {
		checkNullEmpty();
		return port;
	}
	public void setPort(String port) {
		this.port = port;
		checkNullEmpty();
	}
	public String getDatabaseName() {
		checkNullEmpty();
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
		checkNullEmpty();
	}
	public String getSchema() {
		checkNullEmpty();
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
		checkNullEmpty();
	}
	public String getUser() {
		checkNullEmpty();
		return user;
	}
	public void setUser(String user) {
		this.user = user;
		checkNullEmpty();
	}
	public String getPassword() {
		checkNullEmpty();
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
		checkNullEmpty();
	}

	@Override
	public String toString() {
		return " [ip=" + ip + ", port=" + port + ", databaseName=" + databaseName + ", schema=" + schema
				+ ", user=" + user + ", password=" + password + "]";
	}
	
	
	
	
}

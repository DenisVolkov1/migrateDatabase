package connection;

public class PropMSSQLConnection extends PropConnection {


	public PropMSSQLConnection(String ip, String port, String databaseName, String schema, String user,
			String password) {
		super(ip, port, databaseName, schema, user, password);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getStringConnection() {
		super.checkNullEmpty();
		return "jdbc:sqlserver://"+ip+":"+port+";encrypt=false;databaseName="+databaseName+";currentSchema="+schema+";user="+user+";password="+password;
	}

}

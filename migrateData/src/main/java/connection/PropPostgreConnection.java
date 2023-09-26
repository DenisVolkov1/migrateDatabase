package connection;

public class PropPostgreConnection extends PropConnection {

	public PropPostgreConnection(String ip, String port, String databaseName, String schema, String user,
			String password) {
		super(ip, port, databaseName, schema, user, password);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getStringConnection() {
		super.checkNullEmpty();
		return "jdbc:postgresql://"+ip+":"+port+"/"+databaseName+"?currentSchema="+schema+"&user="+user+"&password="+password;
	}

}

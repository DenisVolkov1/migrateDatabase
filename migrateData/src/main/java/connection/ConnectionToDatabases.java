package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.postgresql.jdbc.PgConnection;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

public class ConnectionToDatabases {
	
	public static com.microsoft.sqlserver.jdbc.SQLServerConnection getConnectionToMSSqlServer(PropMSSQLConnection p) throws SQLException {
		String connectionUrl = p.getStringConnection();
		Connection con = null;
		//try {
			con = DriverManager.getConnection(connectionUrl);
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} 
		return (SQLServerConnection) con;
	}
	
	public static org.postgresql.jdbc.PgConnection getConnectionToPostgreSQL(PropPostgreConnection p) {
		String connectionUrl = p.getStringConnection();
		Connection con = null;
		try {
			con = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return (PgConnection) con;
	}

}

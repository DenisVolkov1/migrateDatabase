package run;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.postgresql.jdbc.PgConnection;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import pojo.PropConnection;

public class ConnectionToDatabases {
	
	


	public static com.microsoft.sqlserver.jdbc.SQLServerConnection getConnectionToMSSqlServer(PropConnection p) {
		String connectionUrl = p.getStringConnectionToMSSql();
		Connection con = null;
		try {
			con = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return (SQLServerConnection) con;
	}
	
	public static org.postgresql.jdbc.PgConnection getConnectionToPostgreSQL(PropConnection p) {
//		String connectionUrl = "jdbc:postgresql://localhost:5432/SCPRD?currentSchema=public";
//		String user ="postgres";
//		String password ="sql";
		String connectionUrl = p.getStringConnectionToPostgreSQL();
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

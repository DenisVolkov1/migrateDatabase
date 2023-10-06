package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import org.postgresql.jdbc.PgConnection;
import org.postgresql.shaded.com.ongres.scram.common.bouncycastle.pbkdf2.RuntimeCryptoException;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

public class ConnectionToDatabases {
	
	private static boolean isInitPool;
	private static LinkedBlockingQueue<com.microsoft.sqlserver.jdbc.SQLServerConnection> blockingQueue_MSSQL ;
	private static LinkedBlockingQueue<org.postgresql.jdbc.PgConnection> blockingQueue_POSTGRES ;
	
	public static com.microsoft.sqlserver.jdbc.SQLServerConnection getConnectionToMSSqlServer(PropMSSQLConnection p) throws SQLException {
		String connectionUrl = p.getStringConnection();
		Connection con = null;
			con = DriverManager.getConnection(connectionUrl);

		return (SQLServerConnection) con;
	}
	
	public static org.postgresql.jdbc.PgConnection getConnectionToPostgreSQL(PropPostgreConnection p) throws SQLException {
		String connectionUrl = p.getStringConnection();
		Connection con = null;
			con = DriverManager.getConnection(connectionUrl);

		return (PgConnection) con;
	}
	
	public static com.microsoft.sqlserver.jdbc.SQLServerConnection getConnectionToMSSqlServer_FromPool() throws InterruptedException {
		
		return getConnection_FromPool(com.microsoft.sqlserver.jdbc.SQLServerConnection.class);
	}
	
	public static org.postgresql.jdbc.PgConnection getConnectionToPostgreSQL_FromPool() throws InterruptedException {
		
		return getConnection_FromPool(org.postgresql.jdbc.PgConnection.class);
	}

	private static <T extends Connection> T getConnection_FromPool(Class<T> t) throws InterruptedException {
		if(!(isInitPool)) throw new RuntimeException("Pool is not init!");
		if(org.postgresql.jdbc.PgConnection.class != t && com.microsoft.sqlserver.jdbc.SQLServerConnection.class != t) throw new RuntimeException("Unrecognize connection class !!!");
		long timeOut =0;
		T con = null;
		do {
			if(com.microsoft.sqlserver.jdbc.SQLServerConnection.class == t) {
				con= (T) blockingQueue_MSSQL.poll();
			}
			
			if(org.postgresql.jdbc.PgConnection.class == t) {
				con= (T) blockingQueue_POSTGRES.poll();
			}
			if(con == null) {
				Thread.sleep(500);
				timeOut+=500;
			}
		} while (con == null && timeOut <= 3000);
		System.out.println("timeOut: "+timeOut);
		System.out.println("-pool exists count      MSSQL: "+blockingQueue_MSSQL.size());
		System.out.println("-pool exists count PostgreSQL: "+blockingQueue_POSTGRES.size());
		if(con == null) throw new RuntimeException("Pool connections is exhausted!");
		return con;
	}

	public static void returnConnectionInPool(Connection con) {
		if(con instanceof com.microsoft.sqlserver.jdbc.SQLServerConnection) blockingQueue_MSSQL.add((SQLServerConnection) con); 
		if(con instanceof org.postgresql.jdbc.PgConnection) blockingQueue_POSTGRES.add((PgConnection) con); 
	}
	
	public static void returnConnectionInPool(Connection... con) {
		for (int i = 0; i < con.length; i++) {
			returnConnectionInPool(con[i]);
		}
	}
	
	public static void initPool(PropMSSQLConnection pMssql, PropPostgreConnection pPostgres) throws InterruptedException, SQLException {
		if(isInitPool) throw new RuntimeException("Pool is init arlready!");
		int poolSize =60;
		
		blockingQueue_MSSQL = new LinkedBlockingQueue<SQLServerConnection>(poolSize);
		for (int i = 0; i < poolSize; i++) {
			blockingQueue_MSSQL.put(getConnectionToMSSqlServer(pMssql));
		}
		
		blockingQueue_POSTGRES = new LinkedBlockingQueue<PgConnection>(poolSize);
		for (int i = 0; i < poolSize; i++) {
			blockingQueue_POSTGRES.put(getConnectionToPostgreSQL(pPostgres));
		}
		System.out.println("InitPool: is ready!");
		isInitPool=true;
	}
	
	public static void closePools() throws SQLException {
		for (SQLServerConnection sqlServerConnection : blockingQueue_MSSQL) {
			if(!(sqlServerConnection.isClosed())) sqlServerConnection.close();
		}
		for (PgConnection pgConnection : blockingQueue_POSTGRES) {
			if(!(pgConnection.isClosed())) pgConnection.close();
		}
	}

}

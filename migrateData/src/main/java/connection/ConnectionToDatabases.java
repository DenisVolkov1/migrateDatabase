package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.postgresql.jdbc.PgConnection;
import org.postgresql.shaded.com.ongres.scram.common.bouncycastle.pbkdf2.RuntimeCryptoException;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

public class ConnectionToDatabases {
	
	private static boolean isInitPool;
	private static ConcurrentLinkedQueue<com.microsoft.sqlserver.jdbc.SQLServerConnection> blockingQueue_MSSQL ;
	private static ConcurrentLinkedQueue<org.postgresql.jdbc.PgConnection> blockingQueue_POSTGRES ;
	private static PropMSSQLConnection pMssql_;
	private static PropPostgreConnection pPostgres_;
	
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
	
	
	public static com.microsoft.sqlserver.jdbc.SQLServerConnection getConnectionToMSSqlServer_FromPool() throws InterruptedException, SQLException {
		
		return (SQLServerConnection) getConnection_FromPool(com.microsoft.sqlserver.jdbc.SQLServerConnection.class);
	}
	
	public static org.postgresql.jdbc.PgConnection getConnectionToPostgreSQL_FromPool() throws InterruptedException, SQLException {
		
		return (PgConnection) getConnection_FromPool(org.postgresql.jdbc.PgConnection.class);
	}

	private static <T extends Connection> Connection getConnection_FromPool(Class<T> t) throws InterruptedException, SQLException {
		if(!(isInitPool)) throw new RuntimeException("Pool is not init!");
		if(org.postgresql.jdbc.PgConnection.class != t && com.microsoft.sqlserver.jdbc.SQLServerConnection.class != t) throw new RuntimeException("Unrecognize connection class !!!");
		long timeOut =0;
		Connection con = null;
		do {
			if(com.microsoft.sqlserver.jdbc.SQLServerConnection.class == t) {
				//System.out.println("timeOut:"+timeOut+" -MSSQL="+blockingQueue_MSSQL.size());
				con=  blockingQueue_MSSQL.poll();
			}
			
			if(org.postgresql.jdbc.PgConnection.class == t) {
				//System.out.println("timeOut:"+timeOut+" -PostgreSQL="+blockingQueue_POSTGRES.size());
				con= blockingQueue_POSTGRES.poll();
			}
			if(con == null) {
				Thread.sleep(500);
				timeOut+=500;
			}
		} while (con == null && timeOut <= 3000);
		//System.out.println("timeOut:"+timeOut+" -MSSQL="+blockingQueue_MSSQL.size()+" -PostgreSQL="+blockingQueue_POSTGRES.size());
		if(con == null) {
			if(com.microsoft.sqlserver.jdbc.SQLServerConnection.class == t) return getConnectionToMSSqlServer(pMssql_);
			if(org.postgresql.jdbc.PgConnection.class == t) return getConnectionToPostgreSQL(pPostgres_);
		}
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
		pMssql_ = pMssql; 
		pPostgres_ = pPostgres;
		
		blockingQueue_MSSQL = new ConcurrentLinkedQueue<SQLServerConnection>();
		for (int i = 0; i < poolSize; i++) {
			blockingQueue_MSSQL.add(getConnectionToMSSqlServer(pMssql));
		}
		
		blockingQueue_POSTGRES = new ConcurrentLinkedQueue<PgConnection>();
		for (int i = 0; i < poolSize; i++) {
			blockingQueue_POSTGRES.add(getConnectionToPostgreSQL(pPostgres));
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

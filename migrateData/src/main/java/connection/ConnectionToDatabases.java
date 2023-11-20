package connection;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.postgresql.jdbc.PgConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import pojo.TableInformation;
import pojo.TableInformation.QuantitativeRange;
import util.LOg;
import util.PropertiesInFile;
import util.Util;
import static connection.ConObj.*;

public class ConnectionToDatabases {
	
	private static boolean isInitPool;
	private static com.microsoft.sqlserver.jdbc.SQLServerConnection single_connection_mssql;
	private static org.postgresql.jdbc.PgConnection single_connection_postgres;
	//
	private static ConcurrentLinkedQueue<com.microsoft.sqlserver.jdbc.SQLServerConnection> blockingQueue_MSSQL ;
	private static ConcurrentLinkedQueue<org.postgresql.jdbc.PgConnection> blockingQueue_POSTGRES ;
	private static PropMSSQLConnection pMssql_;
	private static PropPostgreConnection pPostgres_;
	
	private static boolean IS_USE_MULTITHREAD;
	
	static {
		try {
			IS_USE_MULTITHREAD = Util.intToBool(Integer.parseInt( PropertiesInFile.getRunProperties().getProperty("is_use_multithread")));
		} catch (Throwable t) {
			LOg.ERROR(t);
		}
	}
	
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
	
	public static com.microsoft.sqlserver.jdbc.SQLServerConnection getConnectionToMSSqlServer_FromPool(TableInformation tableInformation) throws InterruptedException, SQLException {
		return (SQLServerConnection) getConnection(MSSQL_SERVER, tableInformation);
	}
	
	public static org.postgresql.jdbc.PgConnection getConnectionToPostgreSQL_FromPool(TableInformation tableInformation) throws InterruptedException, SQLException {
		return (PgConnection) getConnection(POSTGRESQL, tableInformation);
	}

	@SuppressWarnings("resource")
	private static <T extends Connection> Connection getConnection(ConObj co,TableInformation tableInformation) throws InterruptedException, SQLException {
		if(!(isInitPool)) throw new RuntimeException("Pool is not init!");
		if(MSSQL_SERVER != co && POSTGRESQL != co) throw new RuntimeException("Unrecognize connection class !!!");
		
		long timeOut =0;
		Connection con = null;
		
		if(IS_USE_MULTITHREAD) {
			do {
				if(MSSQL_SERVER == co) {
					con= blockingQueue_MSSQL.poll();
				}else if(POSTGRESQL == co) {
					con= blockingQueue_POSTGRES.poll();
				}
				if(con == null) {
					Thread.sleep(500);// ждём свободное соединение
					timeOut+=500;
				}
			} while (con == null && timeOut <= 1000);
			if(con == null) {
				if(MSSQL_SERVER == co)    return getConnectionToMSSqlServer(pMssql_);
				else if(POSTGRESQL == co) return getConnectionToPostgreSQL(pPostgres_);
			}
		} else {
			if(MSSQL_SERVER == co) return single_connection_mssql;
			else if(POSTGRESQL == co) return single_connection_postgres;
		}
		return con;
	}

	public static void returnConnectionInPool(Connection con) {
		if(con == null) throw new RuntimeException("Returned connection is NULL!");
		if(con == single_connection_mssql || con == single_connection_postgres) return;
		
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
		
		Instant start = Instant.now();
		int poolSize =30;
		pMssql_ = pMssql; 
		pPostgres_ = pPostgres;
		blockingQueue_MSSQL = new ConcurrentLinkedQueue<SQLServerConnection>();
		blockingQueue_POSTGRES = new ConcurrentLinkedQueue<PgConnection>();
		
		single_connection_mssql = getConnectionToMSSqlServer(pMssql);
		single_connection_postgres = getConnectionToPostgreSQL(pPostgres);
		
		if(IS_USE_MULTITHREAD) {
			for (int i = 0; i < poolSize; i++) {
				blockingQueue_MSSQL.add(getConnectionToMSSqlServer(pMssql));
			}
			for (int i = 0; i < poolSize; i++) {
				blockingQueue_POSTGRES.add(getConnectionToPostgreSQL(pPostgres));
			}
		}

		Instant end = Instant.now();
		long sec = Duration.between(start, end).getSeconds();

		LOg.INFO("-------------------");
		LOg.INFO("InitPool: Готово Заняло: "+Util.printTime(sec));
		LOg.INFO("-------------------");
		isInitPool=true;
	}
	
	public static void closePools() throws SQLException {
		for (SQLServerConnection sqlServerConnection : blockingQueue_MSSQL) {
			if(!(sqlServerConnection.isClosed())) sqlServerConnection.close();
		}
		for (PgConnection pgConnection : blockingQueue_POSTGRES) {
			if(!(pgConnection.isClosed())) pgConnection.close();
		}
		if(!(single_connection_mssql.isClosed())) single_connection_mssql.close();
		if(!(single_connection_postgres.isClosed())) single_connection_mssql.close();
	}

}

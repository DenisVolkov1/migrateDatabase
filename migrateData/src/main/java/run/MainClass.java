package run;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.postgresql.jdbc.PgConnection;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import connection.ConnectionToDatabases;
import connection.PropConnection;
import connection.PropMSSQLConnection;
import connection.PropPostgreConnection;
import mappings.MappingTypes;
import pojo.TableInformation;
import util.LOg;
import util.PropertiesInFile;
import util.WriteLogToFile;

import static mappings.MappingTypes.*;

public class MainClass {
	
	private static final PropMSSQLConnection PROP_MSSQL = new PropMSSQLConnection(PropertiesInFile.getRunProperties());
	private static final PropPostgreConnection PROP_POSTGRES = new PropPostgreConnection(PropertiesInFile.getRunProperties());
	
	//private static final PropMSSQLConnection PROP_MSSQL = new PropMSSQLConnection("localhost", "1434", "SCPRD", "wmwhse1", "sa", "sql");
	//private static final PropPostgreConnection PROP_POSTGRES = new PropPostgreConnection("localhost", "5432", "SCPRD", "wmwhse1", "postgres", "sql");
	
	public static void main(String[] args) {
		
	runMain();
		
		//call();

	}

	private static void runMain() {
		Instant start = Instant.now();
		List<String> listTables = null;
		try {
			LOg.INFO("------------------------------------------------------");
			LOg.INFO("INFO :: "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime())+" :::START:::");
			LOg.INFO("------------------------------------------------------");
			try (Connection connectionMSSql = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); 
					Connection connectionPostgreSQL = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES);) {
						if(connectionMSSql.isValid(5)) LOg.INFO("- Connection established to MSSQL server -");
						if(connectionMSSql.isValid(5)) LOg.INFO("- Connection established to PostgreSQL server -");		
			}
			LOg.INFO("------------------------------------------------------");
			
			try (SQLServerConnection connectionMSSql = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); 
	           	 PgConnection connectionPostgreSQL = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES);) {
				// Транкейтим данные на PosygreSQL для данной схемы.
				truncateAllTablePostgreSQL(connectionPostgreSQL, PROP_POSTGRES.getSchema());
					// Все табл что надо перенести.
					listTables = getAllTableNames(connectionMSSql, PROP_MSSQL.getSchema());
					// 1) данные для формата строки вывода.
					setMaxLenghtTableName(listTables);
					setMaxCountTableVAlue(connectionMSSql,PROP_MSSQL.getSchema());
					 Collections.sort(listTables);
					 	//
						for (int i = 0; i < listTables.size(); i++) {
							String tableName = listTables.get(i);
								migrateTable_FromMSSQLToPosgreSQL(connectionMSSql, connectionPostgreSQL, PROP_MSSQL.getSchema(), PROP_POSTGRES.getSchema(), tableName);
						}
	        }
		} catch (Throwable e) { LOg.ERROR(e); }
		Instant end = Instant.now();
		LOg.INFO("------------------------------------------------------");
		long sec = Duration.between(start, end).getSeconds();
		LOg.INFO("-Завершено: Заняло(всего) "+printTime(sec));
		LOg.INFO("-Всего табл.: "+listTables.size()+" шт.");
		
	}

	private static void setMaxCountTableVAlue(SQLServerConnection conM,String schema) throws SQLException {
		maxLenghtCountNumber = getMaxLenCountMssqlTable(conM, schema);
	}

	private static String printTime(long sec) {
		long hour = sec / 3600;
		long minutes = (sec / 60) - (60 * hour);
		long seconds = sec - (60 * minutes) - (3600 * hour);
				
		return ((hour>0) ? hour+" ч. ":"") + ((minutes>0) ? minutes+" мин. ":"") +seconds+" сек.";
	}

	private static void setMaxLenghtTableName(List<String> listTables) {
		String longest = listTables.stream().
			    max(Comparator.comparingInt(String::length)).get();
			maxLenghtTableName = longest.length();
	}

	private static List<String> intersectionOfTwoLists(List<String> list, List<String> otherList) {
		//Делаем листы в нижнем регистре
		List<String> listToLowerCase = list.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
		
		List<String> otherListToLowerCase = list.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
		//Записываем все совпадения из обоих листов.
		List<String> result = listToLowerCase.stream()
				  .distinct()
				  .filter(otherListToLowerCase::contains)
				  .collect(Collectors.toList());
		return result;
	}
	
	private static String INSERT_INTO_PostgreSQLTable(String schema,String tableName,List<String> columns) {
		String columnsThroughoutComma = String.join(",", columns);
		String QMarks ="";
		for (int i = 0; i < columns.size(); i++) QMarks += "?,";
		QMarks = QMarks.substring(0, QMarks.length()-1);
		
		return "INSERT INTO "+schema+"."+tableName+" (" + columnsThroughoutComma + ") \n"
			 + "OVERRIDING SYSTEM VALUE \n"
			 + "VALUES("+QMarks+") \n";
	}
	private static String SELECT_FROM_MSSQLTable(String schema, String tableName, List<String> columns) {
		String columnsThroughoutComma = String.join(",", columns);
		return "SELECT " + columnsThroughoutComma +" FROM "+schema+"."+tableName;
	}
	private static void truncateAllTablePostgreSQL(PgConnection conP,String schema) throws SQLException {
		List<String> tableNames = getAllTableNames(conP, schema);
		String sql = "";
		for (int i = 0; i < tableNames.size(); i++) {
			
			sql=sql.concat("TRUNCATE "+schema+"."+tableNames.get(i)+" RESTART IDENTITY CASCADE; \n");
		}
		//System.out.println(sql);
		
		Instant start = Instant.now();
		try (Statement statement = conP.createStatement()) {
			  int result = statement.executeUpdate(sql);
		}

		Instant end = Instant.now();
		LOg.INFO("- TRUNCATE CASCADE: All tables to PostgreSQL DONE! (in schema '"+schema+"') Заняло: "+printTime(Duration.between(start, end).getSeconds()));
		LOg.INFO("------------------------------------------------------");
	}
	private static int maxLenghtTableName = 1;
	private static int maxLenghtCountNumber = 1;
	private static void migrateTable_FromMSSQLToPosgreSQL(SQLServerConnection connectionMSSql,PgConnection connectionPostgreSQL,String MSSQLSchema, String postgreSchema, String tableName) throws Throwable {
		// TODO Auto-generated method stub
		List<String> columnNames_MSSQLTable      = getColumnNamesToList(connectionMSSql,        MSSQLSchema, tableName);
		List<String> columnNames_PostgreSQLTable = getColumnNamesToList(connectionPostgreSQL, postgreSchema, tableName);
		List<String> columnNames = intersectionOfTwoLists(columnNames_MSSQLTable, columnNames_PostgreSQLTable);
		
			String selectMSSQLTable = SELECT_FROM_MSSQLTable(MSSQLSchema,tableName,columnNames);
	        String insertPostgreSQLTable = INSERT_INTO_PostgreSQLTable(postgreSchema,tableName,columnNames);
	        
        try (PreparedStatement stmtMSSql = connectionMSSql.prepareStatement(selectMSSQLTable); 
        		PreparedStatement stmtPostgreSQL = connectionPostgreSQL.prepareStatement(insertPostgreSQLTable);) {
            ResultSet rsMSSql = stmtMSSql.executeQuery();
            
            int rowCount = 0;
            Instant start = Instant.now();
            
            	while (rsMSSql.next()) {
            		
            		for (int i = 1; i <= columnNames.size(); i++) {
            			stmtPostgreSQL.setObject(i ,fromJavaTypesToPostgresSql(rsMSSql.getObject(i)));
					}
            		stmtPostgreSQL.addBatch();
            		rowCount++;
            	}
            	int[] res = stmtPostgreSQL.executeBatch();
            	Instant end = Instant.now();
            	int mltn=maxLenghtTableName;
            	int mlcn=maxLenghtCountNumber;
            	//
            	String s = String.format(" (MSSQL) %s.%-"+mltn+"s строк: %-"+mlcn+"d ----> (PostgreSQL) %s.%-"+mltn+"s строк: %-"+mlcn+"d Заняло: %s", MSSQLSchema,tableName,rowCount,postgreSchema,tableName,rowCount,printTime(Duration.between(start, end).getSeconds()));
            	LOg.INFO(s);
        }
	}

	private static List<String> columnNamesFromRS(ResultSet rs) throws SQLException {
		// TODO Auto-generated method stub
		ResultSetMetaData rsmt = rs.getMetaData();
		List<String> result = new ArrayList<String>();
		
		int columnCount = rsmt.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			result.add(rsmt.getColumnName(i));
		}
		return result;
	}
	private static List<String> singleColumnToList(ResultSet rs) throws SQLException {
		List<String> result = new ArrayList<String>();
	
		while (rs.next()) 
			result.add(rs.getString(1));

		return result;
	}
	
	private static void call() {
		try (SQLServerConnection conM = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); Statement stmtM = conM.createStatement();
				PgConnection conP = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES); Statement stmtP = conP.createStatement();){
			 //ResultSet rsM = stmtM.executeQuery(q);
			 //System.out.println(getAllTableNames(conM, "wmwhse1") + " " + getAllTableNames(conM, "wmwhse1").size());
			 //System.out.println(getAllTableNames(conP, "wmwhse1") + " " + getAllTableNames(conP, "wmwhse1").size());
			 
			//System.out.println(getColumnNamesToList(conP, "wmwhse1", "ACCESSORIAL").size()+" :: "+getColumnNamesToList(conP, "wmwhse1", "ACCESSORIAL"));
//			 List<String> n = getAllTableNames(conP, "wmwhse1");
//			 for (String s : n) {
//				 System.out.println(getColumnNamesToList(conP, "wmwhse1", s).size()+" "+s);
//			}
			//System.out.println(getColumnNamesToList(conM, "wmwhse1", "LOC"));
			
			//System.out.println( INSERT_INTO_PostgreSQLTable("ACCESSORIAL", getColumnNamesToList(conP, "wmwhse1", "ACCESSORIAL")));
			//System.out.println( SELECT_FROM_MSSQLTable("ACCESSORIAL", getColumnNamesToList(conP, "wmwhse1", "ACCESSORIAL")));
			
			
			//truncateAllTablePostgreSQL(conP, "wmwhse1");
			
			//WriteLogToFile.log_Write(e1.toString());
			
			System.out.println( getMaxLenCountMssqlTable(conM, "wmwhse1"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static List<String> getAllTableNames(Connection con,String schema) throws SQLException {
		String sql =
		"SELECT TABLE_NAME "+
		"FROM INFORMATION_SCHEMA.TABLES "
		+"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + schema + "'";
		
		try (Statement stmt = con.createStatement();){
			return  singleColumnToList(stmt.executeQuery(sql));
		}
	}

	private static List<String> getColumnNamesToList(Connection con,String toSchema,String tableName) throws SQLException {
		try (Statement stmt = con.createStatement();){
			return columnNamesFromRS(stmt.executeQuery("select * from "+toSchema+"."+tableName+" where 1=2 "));
		}
	}
	
	private static List<TableInformation> getDependensiesTablesPostgreSQL(PgConnection con,String schema,String tableName) throws SQLException {
		List<TableInformation> result = new ArrayList<>();
			String SQL = "SELECT DISTINCT" + 
            		"    ccu.table_schema AS foreign_table_schema, " + 
            		"    ccu.table_name AS foreign_table_name " +
            		"FROM information_schema.table_constraints AS tc " + 
            		"JOIN information_schema.key_column_usage AS kcu " + 
            		"    ON tc.constraint_name = kcu.constraint_name " + 
            		"    AND tc.table_schema = kcu.table_schema " + 
            		"JOIN information_schema.constraint_column_usage AS ccu " + 
            		"    ON ccu.constraint_name = tc.constraint_name " + 
            		"WHERE tc.constraint_type = 'FOREIGN KEY' " + 
            		"    AND tc.table_schema = ? " + 
            		"    AND tc.table_name = ?;";
		try (PreparedStatement stmt = con.prepareStatement(SQL);){
	           stmt.setString(1, schema);
	           stmt.setString(2, tableName);
               ResultSet rs = stmt.executeQuery();
            
            while(rs.next()){
            	String ft_schema = rs.getString("foreign_table_schema");
            	String ft_table = rs.getString("foreign_table_name");
            	result.add(new TableInformation(ft_table, ft_schema));
            }
			return  result;
		}
	}
	private static int getMaxLenCountMssqlTable(SQLServerConnection conM,String schema) throws SQLException {
		int res=0;
		String SQL = ""
				+ "SELECT len(max(s.row_count)) "
				+ "FROM   sys.tables t "
				+ "JOIN   sys.dm_db_partition_stats s"
				+ "  ON t.OBJECT_ID = s.OBJECT_ID "
				+ " AND t.type_desc = 'USER_TABLE' "
				+ " AND SCHEMA_NAME ([schema_id]) = ? "
				+ " AND s.index_id IN (0, 1);";
		
		try (PreparedStatement stmt = conM.prepareStatement(SQL);){
	           stmt.setString(1, schema);
	           ResultSet rs = stmt.executeQuery();
		
	        while(rs.next()){
	        	res = rs.getInt(1);
	        }
		}
		return res;
	}

}

package run;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.postgresql.jdbc.PgConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import connection.ConnectionToDatabases;
import connection.PropMSSQLConnection;
import connection.PropPostgreConnection;
import pojo.TableInformation;
import util.LOg;
import util.PropertiesInFile;
import util.Util;

import static mappings.MappingTypes.*;

public class MainClass {
	
	//private static final PropMSSQLConnection PROP_MSSQL = new PropMSSQLConnection(PropertiesInFile.getRunProperties());
	//private static final PropPostgreConnection PROP_POSTGRES = new PropPostgreConnection(PropertiesInFile.getRunProperties());
	
	private static final PropMSSQLConnection PROP_MSSQL = new PropMSSQLConnection("localhost", "1434", "SCPRD", "wmwhse1", "sa", "sql");
	private static final PropPostgreConnection PROP_POSTGRES = new PropPostgreConnection("localhost", "5432", "SCPRD", "wmwhse1", "postgres", "sql");
	
	private static final boolean IS_ALL_SCHEMAS = Util.intToBool(Integer.parseInt( PropertiesInFile.getRunProperties().getProperty("is_use_all_schemas")));
	private static final boolean IS_USE_MULTITHREAD = Util.intToBool(Integer.parseInt( PropertiesInFile.getRunProperties().getProperty("is_use_multithread")));
	
	private static List<TableInformation> listTables;
	private static Map<String, Integer> totalCountMssqlTable;
	
	public static void main(String[] args) {
		runMain();
			//debug();
	}

	private static void runMain() {
		
		try {
			Instant start = Instant.now();
			
			LOg.INFO("------------------------------------------------------");
			LOg.INFO("INFO :: "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime())+" :::START:::");
			LOg.INFO("------------------------------------------------------");
			try (final Connection conM = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); 
					final Connection conP = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES);) {
						if(conM.isValid(5)) LOg.INFO("- Connection established to MSSQL server -");
						if(conP.isValid(5)) LOg.INFO("- Connection established to PostgreSQL server -");		
			}
			LOg.INFO("------------------------------------------------------");
			
			try (SQLServerConnection conM = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); 
	           	 		PgConnection conP = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES);) {
				//Инит бассеин
				ConnectionToDatabases.initPool(PROP_MSSQL, PROP_POSTGRES);
				//
				if(IS_ALL_SCHEMAS) {				 
					List<String> schemas_MSSQL      = getAllSchemasMSSQLServer(conM);
					List<String> schemas_PostgreSQL = getAllSchemasPostgreSQL(conP);
					List<String> schemas = intersectionOfLists(schemas_MSSQL, schemas_PostgreSQL);
					for (int i = 0; i < schemas.size(); i++) {
						String schema = schemas.get(i);				
						if(IS_USE_MULTITHREAD) migrateTables_multithread(conM, conP, schema, schema);
						migrateTables(conM, conP, schema, schema);
					}	
				} else {
					 if(IS_USE_MULTITHREAD) migrateTables_multithread(conM, conP, PROP_MSSQL.getSchema(), PROP_POSTGRES.getSchema());
					 else migrateTables(conM, conP, PROP_MSSQL.getSchema(), PROP_POSTGRES.getSchema());
				}
				
				Instant end = Instant.now();
				LOg.INFO("------------------------------------------------------");
				long sec = Duration.between(start, end).getSeconds();
				LOg.INFO("-Завершено: Заняло(всего) "+printTime(sec));
	        }
		} catch (Throwable e) { LOg.ERROR(e); }
	}

	private static void migrateTables(SQLServerConnection conM, PgConnection conP,String MSSQLSchema, String postgreSchema) throws Throwable {
		// Транкейтим данные на PosygreSQL для данной схемы.
		truncateAllTablePostgreSQL(conP, postgreSchema);
			// Все табл что надо перенести.
			List<TableInformation> listTableMssql = getAllTableNames(conM, MSSQLSchema);
			List<TableInformation> listTablePostgre = getAllTableNames(conP, postgreSchema);
			listTables = intersectionOf_TableInformation_Lists(listTableMssql, listTablePostgre);
			// 1) данные для формата строки вывода.
			setMaxLenghtTableName(listTables);
			setMaxCountTableVAlue(conM, MSSQLSchema);
		 	//
			for (int i = 0; i < listTables.size(); i++) {
				TableInformation tableInformation = listTables.get(i);
					//migrateTable_FromMSSQLToPosgreSQL(conM, conP, MSSQLSchema, postgreSchema, tableInformation);
					migrateTable_FromMSSQLToPosgreSQL(MSSQLSchema, postgreSchema, tableInformation);
			}
			LOg.INFO("------------------------------------------------------");
			LOg.INFO("-Завершено: Всего табл.: "+listTables.size()+" шт.");
			LOg.INFO("------------------------------------------------------");
	}


	private static void migrateTables_multithread(SQLServerConnection conM, PgConnection conP,String MSSQLSchema, String postgreSchema) throws Throwable {
		// Транкейтим данные на PosygreSQL для данной схемы.
		truncateAllTablePostgreSQL(conP, postgreSchema);
			// Все табл что надо перенести.
			List<TableInformation> listTableMssql = getAllTableNames(conM, MSSQLSchema);
			List<TableInformation> listTablePostgre = getAllTableNames(conP, postgreSchema);
			listTables = intersectionOf_TableInformation_Lists(listTableMssql, listTablePostgre);
			// уст. знач число строк в каждой табл.
			setTotalRowsInTables(conM,MSSQLSchema,listTables);
			// 1) данные для формата строки вывода.
			setMaxLenghtTableName(listTables);
			setMaxCountTableVAlue(conM, MSSQLSchema);
		 	//
			List<Thread> allThread = new ArrayList<Thread>();// Потоки для обработки каждой табл.
			Thread startThreads = new Thread(()->{ // Поток запуска который запускает все потоки
				for (int i = 0; i < listTables.size(); i++) {
					TableInformation tableInformation = listTables.get(i);
					Thread t = new Thread(()->{// Рабочий поток на каждую таблицу из списка
						try {
							migrateTable_FromMSSQLToPosgreSQL(MSSQLSchema, postgreSchema, tableInformation);
						} catch (Throwable e) {
							LOg.INFO("ERR: "+e.getMessage());
							//throw new RuntimeException(e);
						}
					});
					allThread.add(t);
					t.start();
				}
			});
			startThreads.start();
			startThreads.join();// Ждём когда отработает поток по запуску всех потоков
			for (Thread t : allThread) t.join();// Ждём все потоки
			// закрываем все соединения
			ConnectionToDatabases.closePools();
			
			//Thread.currentThread().w
			LOg.INFO("------------------------------------------------------");
			LOg.INFO("-Завершено: Всего табл.: "+listTables.size()+" шт.");
			LOg.INFO("------------------------------------------------------");
	}
	
	private static void setTotalRowsInTables(SQLServerConnection conM,String schema,List<TableInformation> _listTables) throws SQLException {
		totalCountMssqlTable = getMapTotalCountMssqlTable(conM, schema);
		
		for (int i = 0; i < _listTables.size(); i++) {
			String nameTable = listTables.get(i).getTableName();
			int countRows = totalCountMssqlTable.get(nameTable);
				listTables.get(i).setTotalRows(countRows);
		}
	}
	
	private static int maxLenghtTableName = 1;
	private static int maxLenghtCountNumber = 1;
	private static void migrateTable_FromMSSQLToPosgreSQL(String MSSQLSchema, String postgreSchema, TableInformation tableInformation) throws Throwable {
		
		if(!(tableInformation.isEmpty()) & !(tableInformation.inProcess())) {
			
			SQLServerConnection conM = ConnectionToDatabases.getConnectionToMSSqlServer_FromPool(tableInformation); 
	       	PgConnection conP = ConnectionToDatabases.getConnectionToPostgreSQL_FromPool(tableInformation);
			
			
			String tableName = tableInformation.getTableName();
			List<String> listDepTables = getDependensiesTablesPostgreSQL(conP, postgreSchema, tableName);
			
			for (int i = 0; i < listDepTables.size(); i++) {
				TableInformation tiDepTables = getTableInformation(listDepTables.get(i));
				
				//migrateTable_FromMSSQLToPosgreSQL(conM, conP, MSSQLSchema, postgreSchema, ti);
				migrateTable_FromMSSQLToPosgreSQL(MSSQLSchema, postgreSchema, tableInformation);
			}
			
			List<String> columnNames_MSSQLTable      = getColumnNamesToList(conM,        MSSQLSchema, tableName);
			List<String> columnNames_PostgreSQLTable = getColumnNamesToList(conP, postgreSchema, tableName);
			List<String> columnNames = intersectionOfLists(columnNames_MSSQLTable, columnNames_PostgreSQLTable);
			
				String selectMSSQLTable = SELECT_FROM_MSSQLTable(MSSQLSchema,tableName,columnNames);
		        String insertPostgreSQLTable = INSERT_INTO_PostgreSQLTable(postgreSchema,tableName,columnNames);
		        
	        try (PreparedStatement stmtMSSql = conM.prepareStatement(selectMSSQLTable); 
	        		PreparedStatement stmtPostgreSQL = conP.prepareStatement(insertPostgreSQLTable);) {
	            ResultSet rsMSSql = stmtMSSql.executeQuery();
	            ResultSetMetaData rsmd=rsMSSql.getMetaData();	            
	            
	            int rowCount = 0;
	            Instant start = Instant.now();
	            
	            	while (rsMSSql.next()) {
	            		
	            		for (int i = 1; i <= columnNames.size(); i++) {
	            			stmtPostgreSQL.setObject(i ,fromJavaTypesToPostgresSql(rsMSSql.getObject(i),rsmd.getColumnTypeName(i)));
						}
	            		stmtPostgreSQL.addBatch();
	            		rowCount++;
	            	}
	            	try {
		            	int[] res = stmtPostgreSQL.executeBatch();
		            	Instant end = Instant.now();
		            	int mltn=maxLenghtTableName;
		            	int mlcn=maxLenghtCountNumber;
		            	//
		            	String s = String.format(" (MSSQL) %s.%-"+mltn+"s строк: %-"+mlcn+"d ----> (PostgreSQL) %s.%-"+mltn+"s строк: %-"+mlcn+"d Заняло: %s", MSSQLSchema,tableName,rowCount,postgreSchema,tableName,rowCount,printTime(Duration.between(start, end).getSeconds()));
		            	//LOg.INFO(s);
	            	} catch(Throwable t) {
	            		LOg.INFO("ERR: "+tableName+" "+t.getMessage());
	            	}
	        }
	        
	        ConnectionToDatabases.returnConnectionInPool(conP,conM);
		}
	}
	
	private static TableInformation getTableInformation(String tableName) {
		for (int i = 0; i < listTables.size(); i++) {
			if(listTables.get(i).getTableName().equals(tableName)) return listTables.get(i);
		}
		return null;
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

	private static void setMaxLenghtTableName(List<TableInformation> listTables) {
		TableInformation longest = listTables.stream().
			    max(Comparator.comparingInt(TableInformation::length)).get();
			maxLenghtTableName = longest.length();
	}

	private static List<String> intersectionOfLists(List<String> list, List<String> otherList) {
		//Делаем листы в нижнем регистре
		List<String> listToLowerCase = list.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
		
		List<String> otherListToLowerCase = otherList.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
		//Записываем все совпадения из обоих листов.
		List<String> result = listToLowerCase.stream()
				  .distinct()
				  .filter(otherListToLowerCase::contains)
				  .collect(Collectors.toList());
		return result;
	}
	private static List<TableInformation> intersectionOf_TableInformation_Lists(List<TableInformation> list, List<TableInformation> otherList) {
		//Делаем листы в нижнем регистре
		List<TableInformation> listToLowerCase = list.stream()
                .collect(Collectors.toList());
		
		List<TableInformation> otherListToLowerCase = otherList.stream()
                .collect(Collectors.toList());
		//Записываем все совпадения из обоих листов.
		List<TableInformation> result = listToLowerCase.stream()
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
		List<TableInformation> tableNames = getAllTableNames(conP, schema);
		String sql = "";
		for (int i = 0; i < tableNames.size(); i++) {
			
			sql=sql.concat("TRUNCATE "+schema+"."+tableNames.get(i).getTableName()+" RESTART IDENTITY CASCADE; \n");
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
	
	private static void debug() {
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
			List<TableInformation> ti = new ArrayList<TableInformation>();
			for (int i = 0; i < 10; i++) {
				ti.add(new TableInformation("zzz"+i));
			}
			Thread thread1 = new Thread(()->{
				ti.get(3).inProcess();
			});
			Thread thread4 = new Thread(()->{
				ti.get(3).inProcess();
			});
			Thread thread2 = new Thread(()->{
				ti.get(3).inProcess();
			});
			Thread thread3 = new Thread(()->{
				ti.get(3).inProcess();
			});
			Thread thread5 = new Thread(()->{
				ti.get(3).inProcess();
			});
			
			thread1.start();
			thread2.start();
			thread3.start();
			thread4.start();
			thread5.start();
			
//			Thread thread1 = new Thread() {
//
//				@Override
//				public void run() {
//						ti.get(3).getAlreadyProcessed();
//				}
//				
//			};
			
			

		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static List<TableInformation> getAllTableNames(Connection con,String schema) throws SQLException {
		List<TableInformation> result = new ArrayList<>();
		String sql =
		"SELECT LOWER(TABLE_NAME) "+
		"FROM INFORMATION_SCHEMA.TABLES "
		+"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + schema + "' ";
		List<String> listTableName = null;
		try (Statement stmt = con.createStatement();){
			listTableName =  singleColumnToList(stmt.executeQuery(sql));
		}
		
		for (int i = 0; i < listTableName.size(); i++) {
			result.add(new TableInformation(listTableName.get(i)));
		}
		return result;
	}

	private static List<String> getColumnNamesToList(Connection con,String toSchema,String tableName) throws SQLException {
		try (Statement stmt = con.createStatement();){
			return columnNamesFromRS(stmt.executeQuery("select * from "+toSchema+"."+tableName+" where 1=2 "));
		}
	}
	
	private static List<String> getDependensiesTablesPostgreSQL(PgConnection con,String schema,String tableName) throws SQLException {
			String SQL = ""
					+ "SELECT DISTINCT" + 
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
               List<String> result = singleColumnToList(rs);
               //System.out.println(tableName);
               //System.out.println(result);
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
	private static Map<String,Integer> getMapTotalCountMssqlTable(SQLServerConnection conM,String schema) throws SQLException {
		Map<String,Integer> res= new HashMap<String, Integer>();
		String SQL = ""
				+ "SELECT LOWER(t.name) [table] ,s.row_count "
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
	        	res.put(rs.getString("table"), rs.getInt("row_count"));
	        }
		}
		return res;
	}
	private static List<String> getAllSchemasMSSQLServer(SQLServerConnection conM) throws SQLException {
		String sql = ""+
				"select s.name " + 
				"from sys.schemas s " + 
				"    inner join sys.sysusers u " + 
				"        on u.uid = s.principal_id " + 
				"where issqluser=1 and sid is not null";
		
		try (Statement stmt = conM.createStatement();){
			return singleColumnToList(stmt.executeQuery(sql));
		}
	}
	
	private static List<String> getAllSchemasPostgreSQL(PgConnection conP) throws SQLException {
		String sql = "select schema_name FROM information_schema.schemata where schema_name not in ('pg_toast','pg_catalog','information_schema') ";
		
		try (Statement stmt = conP.createStatement();){
			return singleColumnToList(stmt.executeQuery(sql));
		}
	}
}

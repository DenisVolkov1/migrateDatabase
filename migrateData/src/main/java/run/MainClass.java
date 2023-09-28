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
	
	public static void main(String[] args) {
		
	runMain();
		
		//call();


	
	}

	private static void runMain() {
		LOg.INFO("------------------------------------------------------");
		LOg.INFO("INFO :: "+new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(Calendar.getInstance().getTime())+" :::START:::");
		LOg.INFO("------------------------------------------------------");
		try (Connection connectionMSSql = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); 
				Connection connectionPostgreSQL = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES);) {
					if(connectionMSSql.isValid(5)) LOg.INFO("- Connection established to MSSQL server -");
					if(connectionMSSql.isValid(5)) LOg.INFO("- Connection established to PostgreSQL server -");
					LOg.INFO("------------------------------------------------------");
		} catch (Throwable e) { LOg.ERROR(e); }
			
		
//		for (int i = 0; i < bases.length; i++) {
//			try (SQLServerConnection connectionMSSql = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); 
//	           	 PgConnection connectionPostgreSQL = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES);) {
//				
//				truncateAllTablePostgreSQL(connectionPostgreSQL, PROP_POSTGRES.getSchema());
//				
//				
//	        	
//	        	
//	        } catch (Throwable e) {
//	        	LOg.ERROR(e);
//			}
//		}
		
	}

	private static List<String> intersectionOfTwoLists(List<String> list, List<String> otherList) {
		// TODO Auto-generated method stub
		List<String> listToLowerCase = list.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
		
		List<String> otherListToLowerCase = list.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
		
		List<String> result = listToLowerCase.stream()
				  .distinct()
				  .filter(otherListToLowerCase::contains)
				  .collect(Collectors.toList());
		return result;
	}
	
	private static String INSERT_INTO_PostgreSQLTable(String tableName, List<String> columns) {
		String columnsThroughoutComma = String.join(",", columns);
		String QMarks ="";
		for (int i = 0; i < columns.size(); i++) QMarks += "?,";
		QMarks = QMarks.substring(0, QMarks.length()-1);
		
		return "INSERT INTO " + tableName + " (" + columnsThroughoutComma + ") \n"
			 + "OVERRIDING SYSTEM VALUE \n"
			 + "VALUES("+QMarks+") \n";
	}
	private static String SELECT_FROM_MSSQLTable(String tableName, List<String> columns) {
		String columnsThroughoutComma = String.join(",", columns);
		return "SELECT " + columnsThroughoutComma +" FROM "+tableName;
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
		LOg.INFO("- TRUNCATE CASCADE: All tables to PostgreSQL DONE! (in schema '"+schema+"') Заняло: "+Duration.between(start, end).getSeconds()+"сек.");
	}
	private static void migrateTable_FromMSSQLToPosgreSQL(SQLServerConnection connectionMSSql,PgConnection connectionPostgreSQL,String MSSQLSchema, String postgreSchema, String tableName) throws Throwable {
		// TODO Auto-generated method stub
		List<String> columnNames_MSSQLTable      = getColumnNamesToList(connectionMSSql,        MSSQLSchema, tableName);
		List<String> columnNames_PostgreSQLTable = getColumnNamesToList(connectionPostgreSQL, postgreSchema, tableName);
		List<String> columnNames = intersectionOfTwoLists(columnNames_MSSQLTable, columnNames_PostgreSQLTable);
		
			String selectMSSQLTable = SELECT_FROM_MSSQLTable(tableName,      columnNames);
	        String insertPostgreSQLTable = INSERT_INTO_PostgreSQLTable(tableName, columnNames);
	        		            			
        try (Statement stmtMSSql = connectionMSSql.createStatement(); 
        		PreparedStatement stmtPostgreSQL = connectionPostgreSQL.prepareStatement(insertPostgreSQLTable);) {
            ResultSet rsMSSql = stmtMSSql.executeQuery(selectMSSQLTable);
            
            	while (rsMSSql.next()) {
            		
            		for (int i = 1; i <= columnNames.size(); i++) {
            			stmtPostgreSQL.setObject(i ,fromJavaTypesToPostgresSql(rsMSSql.getObject(i)));
					}
//            		stmtPostgreSQL.setObject(1,fromJavaTypesToPostgresSql(rsMSSql.getObject(1)));
//            		stmtPostgreSQL.setObject(2,fromJavaTypesToPostgresSql(rsMSSql.getObject(2)));
//            		stmtPostgreSQL.setObject(3,fromJavaTypesToPostgresSql(rsMSSql.getObject(3)));
//            		stmtPostgreSQL.setObject(4,fromJavaTypesToPostgresSql(rsMSSql.getObject(4)));
//            		stmtPostgreSQL.setObject(5,fromJavaTypesToPostgresSql(rsMSSql.getObject(5)));
//            		stmtPostgreSQL.setObject(6,fromJavaTypesToPostgresSql(rsMSSql.getObject(6)));
//            		stmtPostgreSQL.setObject(7,fromJavaTypesToPostgresSql(rsMSSql.getObject(7)));
//            		stmtPostgreSQL.setObject(8,fromJavaTypesToPostgresSql(rsMSSql.getObject(8)));
//            		stmtPostgreSQL.setObject(9,fromJavaTypesToPostgresSql(rsMSSql.getObject(9)));
            		stmtPostgreSQL.addBatch();
            		
            	}
            	stmtPostgreSQL.executeBatch();

        }
        // Handle any errors that may have occurred.
        catch (Throwable t) {
            t.printStackTrace();
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
			return  columnNamesFromRS(stmt.executeQuery("select * from "+toSchema+"."+tableName+" where 1=2 "));
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

}

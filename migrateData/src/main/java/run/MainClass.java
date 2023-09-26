package run;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.postgresql.jdbc.PgConnection;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import connection.ConnectionToDatabases;
import connection.PropConnection;
import connection.PropMSSQLConnection;
import connection.PropPostgreConnection;
import mappings.MappingTypes;
import pojo.TableInformation;

import static mappings.MappingTypes.*;

public class MainClass {
	
	private static final PropMSSQLConnection PROP_MSSQL = new PropMSSQLConnection("localhost", "1434", "SCPRD", "wmwhse1", "sa", "sql");
	private static final PropPostgreConnection PROP_POSTGRES = new PropPostgreConnection("localhost", "5432", "SCPRD", "wmwhse1", "postgres", "sql");
	
	public static void main(String[] args) {
		 call();
		String bases[] = { "SCPRD" };
		//String schema = "dbo"; 
				
//		for (int i = 0; i < bases.length; i++) {
//			try (Connection connectionMSSql = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); 
//	           	 Connection connectionPostgreSQL = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES);) {
//	        	
//	        	
//	        } catch (Throwable e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}

	}

	private static Set<String> intersectionOfTwoLists(List<String> list, List<String> otherList) {
		// TODO Auto-generated method stub
		List<String> listToLowerCase = list.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
		
		List<String> otherListToLowerCase = list.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
		
		Set<String> result = listToLowerCase.stream()
				  .distinct()
				  .filter(otherListToLowerCase::contains)
				  .collect(Collectors.toSet());
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
	private static void migrateTable_FromMSSQLToPosgreSQL(SQLServerConnection connectionMSSql,PgConnection connectionPostgreSQL,String MSSQLSchema, String postgreSchema, String tableName) throws Throwable {
		// TODO Auto-generated method stub
		
		
		            String s = INSERT_INTO_PostgreSQLTable(tableName, getColumnNamesToList(connectionMSSql, MSSQLSchema, tableName));
		            String s2 = SELECT_FROM_MSSQLTable(tableName, getColumnNamesToList(connectionMSSql, MSSQLSchema, tableName));
		            		
//		            		"INSERT INTO REWORK "//(REWORKNUMBER,DESCRIPTION,TASK,TASKMONETKA,ISDELETED,ADDDATE,ADDWHO,EDITWHO,EDITDATE) "
//            		+ " OVERRIDING SYSTEM VALUE "
//            		+ "VALUES(?,?,?,?,?,?,?,?,?)";
		
		
        try (Statement stmtMSSql = connectionMSSql.createStatement(); 
        		PreparedStatement stmtPostgreSQL = connectionPostgreSQL.prepareStatement(s);) {
            String SQL = "SELECT top 1 *"
            		+ "FROM REWORK where task= 'FBR0082'";
            ResultSet rsMSSql = stmtMSSql.executeQuery(SQL);
      
            List<String> listColumnNames = columnNamesFromRS(rsMSSql);
            

            // Iterate through the data in the result set and display it.
            	while (rsMSSql.next()) {
            		
            		stmtPostgreSQL.setObject(1,fromJavaTypesToPostgresSql(rsMSSql.getObject(1)));
            		stmtPostgreSQL.setObject(2,fromJavaTypesToPostgresSql(rsMSSql.getObject(2)));
            		stmtPostgreSQL.setObject(3,fromJavaTypesToPostgresSql(rsMSSql.getObject(3)));
            		stmtPostgreSQL.setObject(4,fromJavaTypesToPostgresSql(rsMSSql.getObject(4)));
            		stmtPostgreSQL.setObject(5,fromJavaTypesToPostgresSql(rsMSSql.getObject(5)));
            		stmtPostgreSQL.setObject(6,fromJavaTypesToPostgresSql(rsMSSql.getObject(6)));
            		stmtPostgreSQL.setObject(7,fromJavaTypesToPostgresSql(rsMSSql.getObject(7)));
            		stmtPostgreSQL.setObject(8,fromJavaTypesToPostgresSql(rsMSSql.getObject(8)));
            		stmtPostgreSQL.setObject(9,fromJavaTypesToPostgresSql(rsMSSql.getObject(9)));
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
		//String q = "select * from sku where 1=2 ";
		try (SQLServerConnection conM = ConnectionToDatabases.getConnectionToMSSqlServer(PROP_MSSQL); Statement stmtM = conM.createStatement();
				PgConnection conP = ConnectionToDatabases.getConnectionToPostgreSQL(PROP_POSTGRES); Statement stmtP = conP.createStatement();){
			 //ResultSet rsM = stmtM.executeQuery(q);
			 //System.out.println(getAllTableNames(conM, "wmwhse1") + " " + getAllTableNames(conM, "wmwhse1").size());
			 //System.out.println(getAllTableNames(conP, "wmwhse1") + " " + getAllTableNames(conP, "wmwhse1").size());
			 
			System.out.println(getColumnNamesToList(conP, "wmwhse1", "ACCESSORIAL").size()+" :: "+getColumnNamesToList(conP, "wmwhse1", "ACCESSORIAL"));
			//System.out.println(getColumnNamesToList(conM, "wmwhse1", "LOC"));
			
			System.out.println( INSERT_INTO_PostgreSQLTable("ACCESSORIAL", getColumnNamesToList(conP, "wmwhse1", "ACCESSORIAL")));
			System.out.println( SELECT_FROM_MSSQLTable("ACCESSORIAL", getColumnNamesToList(conP, "wmwhse1", "ACCESSORIAL")));
			
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

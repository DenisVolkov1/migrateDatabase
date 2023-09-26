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

import mappings.MappingTypes;
import pojo.PropConnection;
import pojo.TableInformation;

import static mappings.MappingTypes.*;

public class MainClass {
	
	private static final PropConnection PROP_MSSQL = new PropConnection("localhost", "1434", "SCPRD", "wmwhse1", "sa", "sql");
	private static final PropConnection PROP_POSTGRES = new PropConnection("localhost", "5432", "SCPRD", "wmwhse1", "postgres", "sql");
	
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

	private static void migrateTable_FromMSSQLToPosgreSQL(Connection connectionMSSql,PgConnection connectionPostgreSQL,String MSSQLSchema, String postgreSchema, String tableName) throws Throwable {
		// TODO Auto-generated method stub
		
		
		List<TableInformation> dt = getDependensiesTablesPostgreSQL(connectionPostgreSQL, postgreSchema, tableName);
		
		if(!(dt.isEmpty())) {
			String depTable = dt.get(0).getTableName();
			String depSchemaTable = dt.get(0).getTableName();
			
			
			migrateTable_FromMSSQLToPosgreSQL(connectionMSSql, connectionPostgreSQL, MSSQLSchema, depSchemaTable, depTable);
		}
		
		            String s = "INSERT INTO REWORK "//(REWORKNUMBER,DESCRIPTION,TASK,TASKMONETKA,ISDELETED,ADDDATE,ADDWHO,EDITWHO,EDITDATE) "
            		+ " OVERRIDING SYSTEM VALUE "
            		+ "VALUES(?,?,?,?,?,?,?,?,?)";
		
		
        try (Statement stmtMSSql = connectionMSSql.createStatement(); 
        		PreparedStatement stmtPostgreSQL = connectionPostgreSQL.prepareStatement(s);) {
            String SQL = "SELECT top 1 *"
//            		"   REWORKNUMBER," + 
//            		"   DESCRIPTION," + 
//            		"	TASK," + 
//            		"	TASKMONETKA," + 
//            		"	ISDELETED," + 
//            		"	ADDDATE," + 
//            		"	ADDWHO," + 
//            		"	EDITWHO," + 
//            		"	EDITDATE "
            		+ "FROM REWORK where task= 'FBR0082'";
            ResultSet rs = stmtMSSql.executeQuery(SQL);
      
            List<String> listColumnNames = getColumnNames(rs);
            

            // Iterate through the data in the result set and display it.
            	while (rs.next()) {
            		
            		stmtPostgreSQL.setObject(1,fromJavaTypesToPostgresSql(rs.getObject(1)));
            		stmtPostgreSQL.setObject(2,fromJavaTypesToPostgresSql(rs.getObject(2)));
            		stmtPostgreSQL.setObject(3,fromJavaTypesToPostgresSql(rs.getObject(3)));
            		stmtPostgreSQL.setObject(4,fromJavaTypesToPostgresSql(rs.getObject(4)));
            		stmtPostgreSQL.setObject(5,fromJavaTypesToPostgresSql(rs.getObject(5)));
            		stmtPostgreSQL.setObject(6,fromJavaTypesToPostgresSql(rs.getObject(6)));
            		stmtPostgreSQL.setObject(7,fromJavaTypesToPostgresSql(rs.getObject(7)));
            		stmtPostgreSQL.setObject(8,fromJavaTypesToPostgresSql(rs.getObject(8)));
            		stmtPostgreSQL.setObject(9,fromJavaTypesToPostgresSql(rs.getObject(9)));
            		stmtPostgreSQL.addBatch();
            		
            	}
            	stmtPostgreSQL.executeBatch();

        }
        // Handle any errors that may have occurred.
        catch (Throwable t) {
            t.printStackTrace();
        }
		
	}
	private static List<String> getColumnNames(ResultSet rs) throws SQLException {
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
			 System.out.println(getAllTableNames(conM, "wmwhse1") + " " + getAllTableNames(conM, "wmwhse1").size());
			 System.out.println(getAllTableNames(conP, "wmwhse1") + " " + getAllTableNames(conP, "wmwhse1").size());
			 
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
	private static List<String> getAllTablesPostgreSQL(SQLServerConnection conM, String base,String schema) throws SQLException {
		String sql =
		"SELECT TABLE_NAME "+
		"FROM "+base+".INFORMATION_SCHEMA.TABLES "
		+"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + schema + "'";
		
		try (Statement stmt = conM.createStatement();){
			return  singleColumnToList(stmt.executeQuery(sql));
		}
	}

	private static List<String> getColumnNames(Connection con,String toSchema,String tableName) throws SQLException {
		try (Statement stmt = con.createStatement();){
			return  getColumnNames(stmt.executeQuery("select * from "+toSchema+"."+tableName+" where 1=2 "));
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

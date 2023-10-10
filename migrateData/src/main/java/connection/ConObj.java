package connection;

import java.sql.Connection;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

public enum ConObj {
	MSSQL_SERVER(com.microsoft.sqlserver.jdbc.SQLServerConnection.class),
	POSTGRESQL(org.postgresql.jdbc.PgConnection.class);

	private Class<? extends Connection> class1;
	
	ConObj(Class<? extends Connection> class1) {
		this.class1 = class1;
	}

	Class<? extends Connection> getConClass() {
		return class1;
	}
}

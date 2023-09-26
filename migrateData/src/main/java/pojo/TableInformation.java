package pojo;

import java.util.Objects;

public class TableInformation {
	
	private String tableName;
	
	private String schema;
	
	public TableInformation(String tableName, String schema) {
		super();
		this.tableName = tableName;
		this.schema = schema;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(schema, tableName);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableInformation other = (TableInformation) obj;
		return Objects.equals(schema, other.schema) && Objects.equals(tableName, other.tableName);
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getSchema() {
		return schema;
	}



	public void setSchema(String schema) {
		this.schema = schema;
	}

}

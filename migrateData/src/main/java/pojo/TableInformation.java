package pojo;

public class TableInformation {
	
	private String tableName;
	private Boolean alreadyProcessed=false;
	
	public TableInformation(String tableName) {
		super();
		this.tableName = tableName;
	}
		
	public Boolean getAlreadyProcessed() {
		return alreadyProcessed;
	}

	public void setAlreadyProcessed(Boolean alreadyProcessed) {
		this.alreadyProcessed = alreadyProcessed;
	}

	public String getTableName() {
		return tableName;
	}
	public int length() {
		return tableName.length();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((alreadyProcessed == null) ? 0 : alreadyProcessed.hashCode());
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		return result;
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
		if (alreadyProcessed == null) {
			if (other.alreadyProcessed != null)
				return false;
		} else if (!alreadyProcessed.equals(other.alreadyProcessed))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String toString() {
		return "TableInformation [tableName=" + tableName + ", alreadyProcessed=" + alreadyProcessed + "]";
	}



}

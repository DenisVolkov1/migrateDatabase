package pojo;

public class TableInformation {
	
	private String tableName;
	private Boolean alreadyProcessing=false;
	private Boolean isFinishedProcess=false;
	private int totalRows = 0;
	
	public enum QuantitativeRange {		
		LESS_THAN_1000,
		MORE_THAN_1000
	}
	
	public enum FlagProcess {		
		STARTING_PROCESS,
		NOT_START_PROCESS
	}
	
	public TableInformation(String tableName) {
		super();
		this.tableName = tableName;
	}
		
	public Boolean getAlreadyProcessing() {
		return alreadyProcessing;
	}
	
	public synchronized FlagProcess inProcess(boolean isDepTable) throws InterruptedException {
			// Процесс по таблице не запущен.
			if(alreadyProcessing == false) {
				//Запускаем..
				alreadyProcessing = true;
				//возвращаем 
				return FlagProcess.STARTING_PROCESS;
				//System.out.println(Thread.currentThread().getName()+" alreadyProcessed "+alreadyProcessed);
			//Процесс по таблице уже запущен.	
			} else {
				if(isDepTable) {
					while(!(isFinishedProcess)) {
						// Ожидаем обработки таблицы в другом процессе, выходим
						Thread.sleep(200);
						// System.out.println(" Ждём... tableName="+tableName);
					}
					return FlagProcess.NOT_START_PROCESS;
				}
				// Рут таблица без ожиданий, проц не запускаем.
				return FlagProcess.NOT_START_PROCESS;
			}
	}

	public String getTableName() {
		return tableName;
	}
	
	public int length() {
		return tableName.length();
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}

	public int getTotalRows() {
		return totalRows;
	}

	public void setTotalRows(int totalRows) {
		this.totalRows = totalRows;
	}

	public boolean isEmpty() {
		return (getTotalRows() == 0 ? true : false);
	}
	
	public QuantitativeRange getRange() {
		if(getTotalRows() >= 1 && getTotalRows() <= 1000) {
			return TableInformation.QuantitativeRange.LESS_THAN_1000;
		}
		return TableInformation.QuantitativeRange.MORE_THAN_1000;
	}

	public Boolean isFinishedProcess() {
		return isFinishedProcess;
	}

	public void setFinishedProcess() {
		this.isFinishedProcess = true;
	}

	@Override
	public String toString() {
		return " [tableName=" + tableName + ", alreadyProcessing=" + alreadyProcessing
				+ ", isFinishedProcess=" + isFinishedProcess + ", totalRows=" + totalRows + "]";
	}
}

package util;

public class LOg {
	
	public static void INFO(String info) {
		String logInfo = info+"\n";
	        System.out.println(logInfo);
	        WriteLogToFile.log_Write(logInfo);
	}
	public static void ERROR(Throwable error) {
		error.printStackTrace();		
		String stackTraceStrings = "";
		
		stackTraceStrings = "MESSAGE ERROR ::: "+stackTraceStrings.concat(error.getMessage().toString()+"\n");
		stackTraceStrings = stackTraceStrings.concat("-------------");
		for (StackTraceElement iterable_element : error.getStackTrace()) {
			stackTraceStrings = stackTraceStrings.concat(iterable_element.toString()+"\n");
		}

		if(error.getCause() != null) {
			stackTraceStrings = "MESSAGE ERROR CAUSE ::: "+stackTraceStrings.concat(error.getCause().getMessage().toString()+"\n");
			stackTraceStrings = stackTraceStrings.concat("-------------------");
			for (StackTraceElement iterable_element : error.getCause().getStackTrace()) {
				stackTraceStrings = stackTraceStrings.concat(iterable_element.toString()+"\n");
			}
		}
		
        WriteLogToFile.log_Write(stackTraceStrings);
	}

}

package util;

public class LOg {
	
	public static void INFO(String info) {
	        System.out.println(info);
	        WriteLogToFile.log_Write(info+"\n");
	}
	public static void ERROR(Throwable error) {
		error.printStackTrace();		
		String stackTraceStrings = "";
		
		stackTraceStrings = "MESSAGE ERROR ::: "+stackTraceStrings.concat(error.getMessage().toString()+"\n");
		stackTraceStrings = stackTraceStrings.concat("-------------\n");
		for (StackTraceElement iterable_element : error.getStackTrace()) {
			stackTraceStrings = stackTraceStrings.concat(iterable_element.toString()+"\n");
		}

		if(error.getCause() != null) {
			stackTraceStrings = "MESSAGE ERROR CAUSE ::: "+stackTraceStrings.concat(error.getCause().getMessage().toString()+"\n");
			stackTraceStrings = stackTraceStrings.concat("-------------------\n");
			for (StackTraceElement iterable_element : error.getCause().getStackTrace()) {
				stackTraceStrings = stackTraceStrings.concat(iterable_element.toString()+"\n");
			}
		}
		
        WriteLogToFile.log_Write(stackTraceStrings);
	}

}

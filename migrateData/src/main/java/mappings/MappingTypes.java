package mappings;

public class MappingTypes {
	
	public static Object fromJavaTypesToPostgresSql(Object obj) {
		
	    if (obj instanceof java.lang.Boolean) {
	    	return (boolean) (obj) ? 1 : 0;
	    }
		return obj;
	}
}

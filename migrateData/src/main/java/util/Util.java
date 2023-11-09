package util;

public class Util {
	
	public static boolean intToBool(int _0or1) {
		if(_0or1 != 0 && _0or1 != 1) throw new RuntimeException("Zero(O) or one(1) available!");
		return (_0or1 == 1) ? true : false;
	}

	public static String printTime(long sec) {
		long hour = sec / 3600;
		long minutes = (sec / 60) - (60 * hour);
		long seconds = sec - (60 * minutes) - (3600 * hour);
				
		return ((hour>0) ? hour+" ч. ":"") + ((minutes>0) ? minutes+" мин. ":"") +seconds+" сек.";
	}
}

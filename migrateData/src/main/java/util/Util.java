package util;

public class Util {
	
	public static boolean intToBool(int _0or1) {
		if(_0or1 != 0 && _0or1 != 1) throw new RuntimeException("Zero(O) or one(1) available!");
		return (_0or1 == 1) ? true : false;
	}

}

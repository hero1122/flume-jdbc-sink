package com.run;

public class CommonUtils {
	/**
	 * 格式转换
	 * @param smacStr
	 * @return
	 */
	public static String convertMac2String(String mac){
		if(mac.contains("-")){
			mac = mac.replaceAll("-", "");
		}
		if(mac.contains(":")){
			mac = mac.replaceAll(":", "");
		}
		return mac.toLowerCase();
	}
}

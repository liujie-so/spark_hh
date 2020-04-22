package com.ky.util;

import java.util.ResourceBundle;

public class AccommUtil {
	public static final	ResourceBundle PROP_CONFIG = ResourceBundle.getBundle("prop");
	
	public static String getRuntimeRootDir(){
		String path = AccommUtil.class.getProtectionDomain().getCodeSource()
				.getLocation().getFile();
    	if (path.endsWith(".jar")) {
    		int index = path.lastIndexOf("/");
    		return path.substring(0, index);
		}
    	return path;
    }
}

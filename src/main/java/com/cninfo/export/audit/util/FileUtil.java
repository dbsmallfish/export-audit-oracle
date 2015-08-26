package com.cninfo.export.audit.util;

import java.io.File;

public class FileUtil {
	
	
	public static boolean createFile(String destFileName) {

		boolean result = false;
		
		File file = new File(destFileName);
		if(! file.exists())
		{
			result = file.mkdirs();

		}

		return result;

	}
	

}

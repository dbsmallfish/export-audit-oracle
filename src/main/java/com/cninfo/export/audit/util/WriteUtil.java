package com.cninfo.export.audit.util;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class WriteUtil {

	public static void write(String file, String str) {
		OutputStream os = null;
		try {
			os = new BufferedOutputStream(new FileOutputStream(file, true));
			os.write(str.getBytes());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				os.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

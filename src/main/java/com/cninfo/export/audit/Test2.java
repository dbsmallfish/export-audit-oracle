package com.cninfo.export.audit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Test2 {
	private static final Log logger = LogFactory.getLog(Test2.class);

	public static void main(String[] args) {
		FileInputStream input = null;
		try {
			input = new FileInputStream(new File("c:/b.txt"));

			byte[] buf = new byte[1024];

			int len = input.read(buf);
			String str = new String(buf, 0, len);

			System.out.println(str);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
			logger.info("Exception:" + e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				input.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}

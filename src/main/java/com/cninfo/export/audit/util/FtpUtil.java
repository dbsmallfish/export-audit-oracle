package com.cninfo.export.audit.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTPClient;
/**
 * 
 * ftp工具Ftp包类，实现指定文件的上传和下载功能
 * 
 * @author chenyusong Create Date： 2015年7月14日
 * @version 1.0.0
 *
 */

public class FtpUtil {

	/**
	 * 
	 * 用FTP实现对文件的上传
	 * 
	 * @param 文件的路径
	 * 
	 * @return 返回空
	 */
	public static boolean upload(String file) {
		boolean result = false;

		FileInputStream fis = null;
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.connect("172.26.2.93");
			ftpClient.login("mysql", "mysql");
			File localFile = new File(file);

			fis = new FileInputStream(localFile);

			// 设置上传目录

			ftpClient.changeWorkingDirectory("/home/mysql/ftpdir");
			ftpClient.setBufferSize(1024);
			ftpClient.setControlEncoding("GBK");

			// 设置文件类型（二进制）
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			result = ftpClient.storeFile(localFile.getName(), fis);

		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}

			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try {
				ftpClient.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;

	}

	//ftp下载
	public static boolean download(String file) {
		boolean result = false;

		FTPClient ftpClient = new FTPClient();
		FileOutputStream fos = null;

		try {
			ftpClient.connect("172.26.2.93");
			ftpClient.login("mysql", "mysql");

			fos = new FileOutputStream("c:/down.txt");

			ftpClient.setBufferSize(1024);
			// 设置文件类型（二进制）
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			result = ftpClient.retrieveFile(file, fos);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != fos) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
			try {
				ftpClient.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;

	}

}

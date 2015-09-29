package com.cninfo.export.audit.model;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;


/**
 * ClientConfig 类提供了对client.properties文件的操作方法
 * 
 * 
 * @author chenyusong
 * Create Date： 2015年7月21日
 * @version 1.0.0
 *
 */
public class ClientConfig {

	private static final String file = "client.properties";

	private static final String AUTH_INFO = "{0}/{1}@{2}:{3}/{4}";

	private static final String KEY_INFO = "oracle.{0}";

	private static ClientConfig config = new ClientConfig();;

	private Properties properties;

	public static ClientConfig getSingleton() {
		return config;
	}

	// 封装了一个Properties类，用来读取oracle数据库的info
	private ClientConfig() {
		properties = new Properties();
		InputStream in = ClientConfig.class.getClassLoader()
				.getResourceAsStream(file);
		try {
			//将Properties文件的内容加载到properties对象中
			properties.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// 根据key获取value
	public String get(String key) {
		return properties.getProperty(key);
	}

	// 根据key设置value
	public void set(String key, String value) {
		properties.setProperty(key, value);

	}

	// 根据key获取数据库info，传递的key的取值为数组，比如1,2,3
	public AuthInfo getAuthInfo(String key) {
		AuthInfo authInfo = new AuthInfo();

		MessageFormat formatKeyInfo = new MessageFormat(KEY_INFO);

		String keyInfo = formatKeyInfo.format(new Object[]{key});

		String info = config.get(keyInfo);

		MessageFormat mf = new MessageFormat(AUTH_INFO);
		try {
			Object[] values = mf.parse(info);
			authInfo.setUser(values[0].toString());
			authInfo.setPassword(values[1].toString());
			authInfo.setHost(values[2].toString());
			authInfo.setPort(Integer.parseInt(values[3].toString()));
			authInfo.setSid(values[4].toString());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return authInfo;
	}

	// 获取所有的"oracle.{0}" 样式的key
	public Set<String> getNames() {
		Set<String> names = new HashSet<String>();
		MessageFormat mf = new MessageFormat(KEY_INFO);
		for (Object o : this.properties.keySet()) {
			String key = o.toString();
			if (key.matches(mf.format(new Object[]{"\\S+?"}))) {
				try {
					Object[] objs = mf.parse(key);
					names.add((String) objs[0]);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		return names;
	}

	// 将内存中的value更新在client.properties配置文件中
	public synchronized void save() {

		String path = ClientConfig.class.getClassLoader()
				.getResource("client.properties").getPath();

		OutputStream fos = null;
		try {

			fos = new BufferedOutputStream(new FileOutputStream(path));
			this.properties.store(fos, "Update  lasttime");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != fos) {
					fos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/*
	 * 
	 * public static void main(String[] args) { ClientConfig conf =
	 * ClientConfig.getSingleton(); System.out.println(conf.get("oracle.7"));
	 * conf.set("lasttime.7", "2012-12-12 08:30:54"); conf.save(); }
	 */
}

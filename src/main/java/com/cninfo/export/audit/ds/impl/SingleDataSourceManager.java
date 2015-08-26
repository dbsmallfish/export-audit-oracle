package com.cninfo.export.audit.ds.impl;

import java.text.MessageFormat;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.cninfo.export.audit.model.AuthInfo;

public class SingleDataSourceManager extends AbstractDataSourceManager {

	// 类加载的时候会初始化成员变量
	private static SingleDataSourceManager singleton = new SingleDataSourceManager();

	private SingleDataSourceManager() {
		super();

	}

	public static SingleDataSourceManager getSingleton() {
		return singleton;

	}

	/**
	 * 根据config配置文件，生成每个key对应的DataSource，该DataSource可以获取Connection的信息
	 */
	@Override
	protected void initDataSources() {
		for (String key : config.getNames()) {
			AuthInfo auth = config.getAuthInfo(key);
			MessageFormat mf = new MessageFormat(URL);
			String url = mf.format(new Object[]{auth.getHost(),
					auth.getPort() + "", auth.getSid()});
			DriverManagerDataSource ds = new DriverManagerDataSource(url,
					auth.getUser(), auth.getPassword());
			ds.setDriverClassName(DRIVER_CLASS);
			map.put(key, ds);
		}
	}

}

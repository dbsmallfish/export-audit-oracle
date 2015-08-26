package com.cninfo.export.audit.ds.impl;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.cninfo.export.audit.ds.DataSourceManager;
import com.cninfo.export.audit.model.ClientConfig;

public abstract class AbstractDataSourceManager implements DataSourceManager {

	protected static final String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
	protected static final String URL = "jdbc:oracle:thin:@{0}:{1}:{2}";

	protected Map<String, DataSource> map;
	protected ClientConfig config;

	public AbstractDataSourceManager() {

		this.map = new HashMap<String, DataSource>();
		this.config = ClientConfig.getSingleton();

		//调用子类的override initDataSources方法
		initDataSources();
	}

	public DataSource getDataSource(String key) {
		return map.get(key);
	}

	/**
	 * 
	 * 根据配置文件中的key，获取数据库的连接信息
	 * 
	 * @param key
	 *            配置文件中的key
	 * @return 返回一个存储了数据库连接信息的javaBean
	 */

	

	protected abstract void initDataSources();

}

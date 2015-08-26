package com.cninfo.export.audit.ds;

import javax.sql.DataSource;

/**
 * <描述>
 * 
 * @author chenyusong
 * Create Date： 2015年7月9日
 * @version 1.0.0
 * 
 */
public interface DataSourceManager {
	/**
	 * 获取DataSource对象，该对象能得到Connection
	 * @param <name>  <描述>
	 * @throws <异常描述>
	 * @return <返回值描述>
	 */
	public DataSource getDataSource(String key);

}

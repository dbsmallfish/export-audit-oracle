package com.cninfo.export.audit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cninfo.export.audit.ds.DataSourceManager;
import com.cninfo.export.audit.ds.impl.SingleDataSourceManager;
import com.cninfo.export.audit.model.ClientConfig;
import com.cninfo.export.audit.util.FileUtil;
import com.cninfo.export.audit.util.WriteUtil;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * 
 * 将oracle审计日志导出成json格式
 * 
 * @author chenyusong Create Date： 2015年7月21日
 * @version 1.0.0
 *
 */
public class ExportTaskOfJson implements Runnable {

	private static final Log logger = LogFactory.getLog(ExportTaskOfJson.class);

	private ClientConfig config;
	private String key;

	// 获取时间的sql语句
	private final String dateSql = "SELECT to_char(MAX(timestamp),'YYYYMMDDHH24miss') THISTIMESTAMP,  to_char(MAX(timestamp)-1/720,'YYYYMMDDHH24miss') LASTTHISTIMESTAMP\r\n"
			+ "from sys.dba_audit_trail";

	// 构造方法
	public ExportTaskOfJson(String key) {
		this.config = ClientConfig.getSingleton();
		this.key = key;

	}

	// 将查询结果保存为json格式
	public BasicDBList queryForDBList(String sql, DataSource ds) {
		List<Map<String, Object>> rs = queryForList(sql, ds);
		return listToDBList(rs);
	}

	// 将查询结果保存为json格式
	public BasicDBList listToDBList(List<Map<String, Object>> re) {
		BasicDBList list = new BasicDBList();
		for (Map<String, Object> row : re) {
			DBObject r = new BasicDBObject();
			for (String key : row.keySet()) {
				r.put(key.toLowerCase(), row.get(key));
			}
			list.add(r);
		}
		return list;
	}

	// 将查询结果保存为json格式
	public List<Map<String, Object>> queryForList(String sql, DataSource ds) {
		JdbcTemplate template = new JdbcTemplate(ds);
		List<Map<String, Object>> rs = template.queryForList(sql);
		return rs;
	}

	// 线程要执行的方法

	public void run() {

		DataSourceManager dm = SingleDataSourceManager.getSingleton();
		String hostDir = config.getAuthInfo(key).getHost();

		logger.info(hostDir + " export audit mission has start ......");

		// 获取key对应的数据库连接
		DataSource ds = dm.getDataSource(key);

		if (null != ds) {
			logger.info("get " + hostDir + " DataSource succeed ......");

		} else {
			logger.info("get " + hostDir + " DataSource failed ......");

		}

		// 获取key对应的上次运行任务的时间
		String lasttime = config.get("lasttime." + key);

		// 获取key对应的ip地址，根据ip地址来创建目录，比如172.30.3.26

		Connection conn = null;
		Statement stat = null;
		ResultSet resultSet = null;
		String THISTIMESTAMP = "";
		String LASTTHISTIMESTAMP = "";

		try {

			conn = ds.getConnection();
			if (null != conn) {
				logger.info("get " + hostDir + " connection succeed ......");

			} else {
				logger.info("get " + hostDir + " connection failed ......");

			}

			stat = conn.createStatement();

			if (null != stat) {
				logger.info("get " + hostDir + " statement succeed ......");

			} else {
				logger.info("get " + hostDir + " statement failed ......");

			}

		} catch (SQLException e2) {
			e2.printStackTrace();
		}

		// 任务首次运行导出任务，由于上次运行日期为空，LASTTHISTIMESTAMP从数据库中查询获取
		if (null == lasttime || lasttime.length() == 0) {

			try {
				resultSet = stat.executeQuery(dateSql);

				if (null != resultSet) {
					logger.info("get " + hostDir
							+ " dateResultSet succeed ......");

				} else {
					logger.info("get " + hostDir
							+ " dateResultSet failed ......");

				}

				resultSet.next();
				THISTIMESTAMP = resultSet.getString("THISTIMESTAMP");
				LASTTHISTIMESTAMP = resultSet.getString("LASTTHISTIMESTAMP");

			} catch (SQLException e) {
				e.printStackTrace();
			}
			// 如果不是首次运行导出任务，则LASTTHISTIMESTAMP从配置文件中获取
		} else {
			try {
				resultSet = stat.executeQuery(dateSql);

				if (null != resultSet) {
					logger.info("get " + hostDir
							+ " dateResultSet succeed ......");

				} else {
					logger.info("get " + hostDir
							+ " dateResultSet failed ......");

				}

				resultSet.next();
				THISTIMESTAMP = resultSet.getString("THISTIMESTAMP");
				LASTTHISTIMESTAMP = lasttime;

			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		// 获取月日小时分钟的信息作为文件名的一部分，比如07161714
		// String filenameBefore = LASTTHISTIMESTAMP.substring(4, 12);
		// 获取月日小时分钟的信息作为文件名的一部分，比如07161715
		// String filenameAfter = THISTIMESTAMP.substring(4, 12);

		// 以年月日来创建目录，比如20150716
		String datedir = LASTTHISTIMESTAMP.substring(0, 8);

		// 审计SQL
		String auditSQL = "select\r\n"
				+ "os_username,username,userhost,terminal,to_char(TIMESTAMP,'YYYY/MM/DD HH24:MI:SS') TIMESTAMP,\r\n"
				+ "owner,obj_name,action,action_name,new_owner,new_name,obj_privilege,\r\n"
				+ "sys_privilege,admin_option,grantee,audit_option,ses_actions,\r\n"
				+ "to_char(logoff_time,'YYYY/MM/DD HH24:MI:SS') logoff_time,\r\n"
				+ "logoff_lread,logoff_pread,logoff_lwrite,logoff_dlock,\r\n"
				+ "comment_text,sessionid,entryid,statementid,\r\n"
				+ "returncode,priv_used,client_id,econtext_id,session_cpu,\r\n"
				+ "to_char(extended_timestamp,'YYYY/MM/DD HH24:MI:SS') extended_timestamp,\r\n"
				+ "proxy_sessionid,global_uid,\r\n"
				+ "instance_number,os_process,transactionid,SCN,sql_bind,sql_text\r\n"
				+ "from sys.dba_audit_trail\r\n"
				+ "where timestamp >= to_date('" + LASTTHISTIMESTAMP
				+ "','YYYYMMDDHH24miss')\r\n" + "AND  timestamp <= to_date('"
				+ THISTIMESTAMP + "','YYYYMMDDHH24miss') ";
		// 审计日志的查询结果

		logger.info(hostDir + " start get AuditResultSet");
		DBObject result = queryForDBList(auditSQL, ds);

		if (null != result) {
			logger.info("get " + hostDir + " AuditResultSet succeed ......");

		} else {
			logger.info("get " + hostDir + " AuditResultSet failed ......");

		}

		if (null != resultSet) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (stat != null)
			try {
				stat.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		// 将本次的最大时间跟新到配置文件中
		config.set("lasttime." + key, THISTIMESTAMP);

		// 通过给VM传递参数获取，一般是放程序的当前目录
		String baseDir = System.getProperty("base.home");

		String writeDir = baseDir + "/oracle_audit/" + hostDir + "/" + datedir
				+ "/";

		// 创建目录
		FileUtil.createFile(writeDir);

		// 将result的结果写入到文件中
		WriteUtil.write(writeDir + config.getAuthInfo(key).getSid() + "_"
				+ LASTTHISTIMESTAMP + "_" + THISTIMESTAMP + ".txt",
				result.toString());

		config.save();

		logger.info(hostDir + " export audit mission finished ......");

	}

}

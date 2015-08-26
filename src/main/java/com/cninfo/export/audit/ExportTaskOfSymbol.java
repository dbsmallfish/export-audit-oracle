package com.cninfo.export.audit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cninfo.export.audit.ds.DataSourceManager;
import com.cninfo.export.audit.ds.impl.SingleDataSourceManager;
import com.cninfo.export.audit.model.ClientConfig;
import com.cninfo.export.audit.util.FileUtil;
import com.cninfo.export.audit.util.WriteUtil;

public class ExportTaskOfSymbol implements Runnable {

	private static final Log logger = LogFactory
			.getLog(ExportTaskOfSymbol.class);

	private ClientConfig config;
	private String key;

	// 获取时间的sql语句
	private final String dateSql = "SELECT to_char(MAX(timestamp),'YYYYMMDDHH24miss') THISTIMESTAMP,  to_char(MAX(timestamp)-1/24,'YYYYMMDDHH24miss') LASTTHISTIMESTAMP\r\n"
			+ "from sys.dba_audit_trail";

	public ExportTaskOfSymbol(String key) {
		this.config = ClientConfig.getSingleton();
		this.key = key;

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
		Statement dateStat = null;
		Statement auditStat = null;

		ResultSet AuditResultSet = null;
		ResultSet DateResultSet = null;

		String THISTIMESTAMP = "";
		String LASTTHISTIMESTAMP = "";

		try {
			conn = ds.getConnection();
			if (null != conn) {
				logger.info("get " + hostDir + " connection succeed ......");

			} else {
				logger.info("get " + hostDir + " connection failed ......");

			}

			dateStat = conn.createStatement();
			auditStat = conn.createStatement();
			if (null != dateStat) {
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
				DateResultSet = dateStat.executeQuery(dateSql);
				if (null != DateResultSet) {
					logger.info("get " + hostDir
							+ " dateResultSet succeed ......");

				} else {
					logger.info("get " + hostDir
							+ " dateResultSet failed ......");

				}
				DateResultSet.next();
				THISTIMESTAMP = DateResultSet.getString("THISTIMESTAMP");
				LASTTHISTIMESTAMP = DateResultSet
						.getString("LASTTHISTIMESTAMP");

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (DateResultSet != null)
					try {
						DateResultSet.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				if (dateStat != null)
					try {
						dateStat.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
			}

			// 如果不是首次运行导出任务，则LASTTHISTIMESTAMP从配置文件中获取

		} else {
			try {
				DateResultSet = dateStat.executeQuery(dateSql);

				if (null != DateResultSet) {
					logger.info("get " + hostDir
							+ " dateResultSet succeed ......");

				} else {
					logger.info("get " + hostDir
							+ " dateResultSet failed ......");

				}
				DateResultSet.next();
				THISTIMESTAMP = DateResultSet.getString("THISTIMESTAMP");
				LASTTHISTIMESTAMP = lasttime;

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (DateResultSet != null)
					try {
						DateResultSet.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				if (dateStat != null)
					try {
						dateStat.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
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
		try {
			AuditResultSet = auditStat.executeQuery(auditSQL);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		if (null != AuditResultSet) {
			logger.info("get " + hostDir + " AuditResultSet succeed ......");

		} else {
			logger.info("get " + hostDir + " AuditResultSet failed ......");
		}

		StringBuffer wirteInfo = new StringBuffer();

		try {
			while (AuditResultSet.next()) {

				wirteInfo.append(AuditResultSet.getString("OS_USERNAME"))
						.append("<#>")
						.append(AuditResultSet.getString("USERNAME"))
						.append("<#>")
						.append(AuditResultSet.getString("USERHOST"))
						.append("<#>")
						.append(AuditResultSet.getString("TERMINAL"))
						.append("<#>")
						.append(AuditResultSet.getString("TIMESTAMP"))
						.append("<#>")
						.append(AuditResultSet.getString("OWNER"))
						.append("<#>")
						.append(AuditResultSet.getString("OBJ_NAME"))
						.append("<#>")
						.append(AuditResultSet.getString("ACTION"))
						.append("<#>")
						.append(AuditResultSet.getString("ACTION_NAME"))
						.append("<#>")
						.append(AuditResultSet.getString("NEW_OWNER"))
						.append("<#>")
						.append(AuditResultSet.getString("NEW_NAME"))
						.append("<#>")
						.append(AuditResultSet.getString("OBJ_PRIVILEGE"))
						.append("<#>")
						.append(AuditResultSet.getString("SYS_PRIVILEGE"))
						.append("<#>")
						.append(AuditResultSet.getString("ADMIN_OPTION"))
						.append("<#>")
						.append(AuditResultSet.getString("GRANTEE"))
						.append("<#>")
						.append(AuditResultSet.getString("AUDIT_OPTION"))
						.append("<#>")
						.append(AuditResultSet.getString("SES_ACTIONS"))
						.append("<#>")
						.append(AuditResultSet.getString("LOGOFF_TIME"))
						.append("<#>")
						.append(AuditResultSet.getString("LOGOFF_LREAD"))
						.append("<#>")
						.append(AuditResultSet.getString("LOGOFF_PREAD"))
						.append("<#>")
						.append(AuditResultSet.getString("LOGOFF_LWRITE"))
						.append("<#>")
						.append(AuditResultSet.getString("LOGOFF_DLOCK"))
						.append("<#>")
						.append(AuditResultSet.getString("COMMENT_TEXT"))
						.append("<#>")
						.append(AuditResultSet.getString("SESSIONID"))
						.append("<#>")
						.append(AuditResultSet.getString("ENTRYID"))
						.append("<#>")
						.append(AuditResultSet.getString("STATEMENTID"))
						.append("<#>")
						.append(AuditResultSet.getString("RETURNCODE"))
						.append("<#>")
						.append(AuditResultSet.getString("PRIV_USED"))
						.append("<#>")
						.append(AuditResultSet.getString("CLIENT_ID"))
						.append("<#>")
						.append(AuditResultSet.getString("ECONTEXT_ID"))
						.append("<#>")
						.append(AuditResultSet.getString("SESSION_CPU"))
						.append("<#>")
						.append(AuditResultSet.getString("EXTENDED_TIMESTAMP"))
						.append("<#>")
						.append(AuditResultSet.getString("PROXY_SESSIONID"))
						.append("<#>")
						.append(AuditResultSet.getString("GLOBAL_UID"))
						.append("<#>")
						.append(AuditResultSet.getString("INSTANCE_NUMBER"))
						.append("<#>")
						.append(AuditResultSet.getString("OS_PROCESS"))
						.append("<#>")
						.append(AuditResultSet.getString("TRANSACTIONID"))
						.append("<#>").append(AuditResultSet.getString("SCN"))
						.append("<#>")
						.append(AuditResultSet.getString("SQL_BIND"))
						.append("<#>")
						.append(AuditResultSet.getString("SQL_TEXT"))
						.append("\n");

			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (AuditResultSet != null)
				try {
					AuditResultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if (auditStat != null)
				try {
					auditStat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

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
				wirteInfo.toString());

		config.save();

		logger.info(hostDir + " export audit mission finished ......");
	}

}

package com.cninfo.export.audit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cninfo.export.audit.ds.DataSourceManager;
import com.cninfo.export.audit.ds.impl.SingleDataSourceManager;
import com.cninfo.export.audit.model.ClientConfig;
import com.cninfo.export.audit.util.FileUtil;
import com.cninfo.export.audit.util.WriteUtil;

public class CopyOfExportTaskOfSymbol implements Runnable {

	private static final Log logger = LogFactory
			.getLog(CopyOfExportTaskOfSymbol.class);

	private ClientConfig config;
	private String key;

	// 获取时间的sql语句
	private final String dateSql = "SELECT to_char(MAX(timestamp),'YYYYMMDDHH24mi') THISTIMESTAMP,to_char(MAX(timestamp)-1/36,'YYYYMMDDHH24mi') LASTTIMESTAMP   from sys.dba_audit_trail";

	public CopyOfExportTaskOfSymbol(String key) {
		this.config = ClientConfig.getSingleton();
		this.key = key;

	}

	public String getDateAfter(String str, int minutes) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

		Date beforeDate = sdf.parse(str);

		Calendar cal = Calendar.getInstance();

		cal.setTime(beforeDate);

		cal.add(Calendar.MINUTE, minutes);

		Date afterDate = cal.getTime();

		String oneHourAfter = sdf.format(afterDate);
		return oneHourAfter;

	}

	public int getDiffSecond(String thisTime, String lastTime)
			throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
		Date d1 = df.parse(thisTime);
		Date d2 = df.parse(lastTime);

		int second = (int) (d1.getTime() - d2.getTime()) / (1000 * 60);
		return second;

	}

	// 线程要执行的方法

	public void run() {
		DataSourceManager dm = SingleDataSourceManager.getSingleton();
		String hostDir = config.getAuthInfo(key).getHost();

		// 获取key对应的数据库连接

		DataSource ds = dm.getDataSource(key);

		// 获取key对应的上次运行任务的时间
		String lasttime = config.get("lasttime." + key);
		// 获取key对应的ip地址，根据ip地址来创建目录，比如172.30.3.26

		Connection conn = null;
		Statement dateStat = null;
		Statement auditStat = null;

		ResultSet AuditResultSet = null;
		ResultSet DateResultSet = null;

		String THISTIMESTAMP = "";
		String LASTTIMESTAMP = "";

		try {
			conn = ds.getConnection();
			dateStat = conn.createStatement();
			auditStat = conn.createStatement();

		} catch (SQLException e2) {
			e2.printStackTrace();
		}
		// 任务首次运行导出任务，由于上次运行日期为空，LASTTHISTIMESTAMP从数据库中查询获取

		if (null == lasttime || lasttime.length() == 0) {

			try {
				DateResultSet = dateStat.executeQuery(dateSql);
				DateResultSet.next();
				THISTIMESTAMP = DateResultSet.getString("THISTIMESTAMP");
				LASTTIMESTAMP = DateResultSet.getString("LASTTIMESTAMP");

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
			LASTTIMESTAMP = lasttime;
			try {
				THISTIMESTAMP = getDateAfter(LASTTIMESTAMP, 5);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		// 获取月日小时分钟的信息作为文件名的一部分，比如07161714
		// String filenameBefore = LASTTHISTIMESTAMP.substring(4, 12);
		// 获取月日小时分钟的信息作为文件名的一部分，比如07161715
		// String filenameAfter = THISTIMESTAMP.substring(4, 12);

		int interval = 0;
		int times = 0;
		try {
			interval = getDiffSecond(THISTIMESTAMP, LASTTIMESTAMP);
			times = interval / 5;

			logger.info(hostDir + " export total times is " + interval);
			logger.info("this export start time is :" + LASTTIMESTAMP);
			logger.info("this export end time is :" + THISTIMESTAMP);
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// 以年月日来创建目录，比如20150716
		String datedir = LASTTIMESTAMP.substring(0, 8);
		// 审计SQL

		for (int i = 0; i < times; i++) {

			String auditSQL = "select\r\n"
					+ "os_username,username,userhost,terminal,to_char(TIMESTAMP,'YYYY/MM/DD HH24:MI:SS') TIMESTAMP,\r\n"
					+ "owner,obj_name,action,action_name,new_owner,new_name,obj_privilege,\r\n"
					+ "sys_privilege,admin_option,grantee,audit_option,ses_actions,\r\n"
					+ "to_char(logoff_time,'YYYY/MM/DD HH24:MI:SS') logoff_time,\r\n"
					+ "logoff_lread,logoff_pread,logoff_lwrite,logoff_dlock,\r\n"
					+ "comment_text,sessionid,entryid,statementid,\r\n"
					+ "returncode,priv_used,client_id,econtext_id,session_cpu,\r\n"
					+ "to_char(extended_timestamp,'YYYY/MM/DD HH24:MI:SS') extended_timestamp,\r\n"
					+ "proxy_sessionid,global_uid\r\n"
					+ "from sys.dba_audit_trail\r\n"
					+ "where  timestamp >= (to_date('" + LASTTIMESTAMP
					+ "','YYYYMMDDHH24mi') +" + i
					+ "*1/288  )  AND  timestamp <( to_date('" + LASTTIMESTAMP
					+ "','YYYYMMDDHH24mi') +(" + i + "+1)*1/288 )\r\n" + " ";

			try {
				AuditResultSet = auditStat.executeQuery(auditSQL);
				logger.debug(auditSQL);

			} catch (SQLException e1) {
				logger.error("Exception:" + hostDir + ":" + e1.getMessage());
				e1.printStackTrace();

			}

			StringBuffer wirteInfo = new StringBuffer();

			logger.info(hostDir + " " + i + " times write start");

			try {
				while (AuditResultSet.next()) {

					wirteInfo
							.append(AuditResultSet.getString("OS_USERNAME"))
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
							.append(AuditResultSet
									.getString("EXTENDED_TIMESTAMP"))
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
							.append("<#>")
							.append(AuditResultSet.getString("SCN"))
							.append("<#>")
							.append(AuditResultSet.getString("SQL_BIND"))
							.append("<#>")
							.append(AuditResultSet.getString("SQL_TEXT"))
							.append("\n");

				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// 通过给VM传递参数获取，一般是放程序的当前目录
			String baseDir = System.getProperty("base.home");

			String writeDir = baseDir + "/oracle_audit/" + hostDir + "/"
					+ datedir + "/";

			// 创建目录
			FileUtil.createFile(writeDir);

			// 将result的结果写入到文件中
			WriteUtil.write(writeDir + config.getAuthInfo(key).getSid() + "_"
					+ LASTTIMESTAMP + "_" + THISTIMESTAMP + ".txt",
					wirteInfo.toString());

			logger.info(hostDir + " " + i + " times write succeed");

		}

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

		// 将本次的最大时间跟新到配置文件中
		config.set("lasttime." + key, THISTIMESTAMP);
		config.save();
		logger.info("lasttime is : " + THISTIMESTAMP);

		logger.info(hostDir + " export audit mission finished  ......\n\n\n");
	}

}

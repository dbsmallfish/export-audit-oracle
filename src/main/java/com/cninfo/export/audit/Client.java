package com.cninfo.export.audit;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cninfo.export.audit.model.ClientConfig;

public class Client {

	private static final Log logger = LogFactory.getLog(Client.class);

	public static void main(String[] args) {

		// 根据配置文件，得到key的集合
		Set<String> keySet = ClientConfig.getSingleton().getNames();

		if (keySet != null) {
			logger.info("get kyes " + keySet.toString() + " succeed ...");

		}

		// 遍历key集合，为每个key生成一个独立的线程去运行ExportTaskOfJson任务
		for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext();) {
			String key = iterator.next();

			ScheduledExecutorService exportService = Executors
					.newSingleThreadScheduledExecutor();

			exportService.scheduleWithFixedDelay(new ExportTaskOfSymbol(key), 0,
					Integer.parseInt(System.getProperty("timeInterval")),
					TimeUnit.MINUTES);

		}

		// ScheduledExecutorService exportService = Executors
		// .newSingleThreadScheduledExecutor();
		//
		// exportService.scheduleAtFixedRate(new ExportTaskOfJson("2"), 0,
		// Integer.parseInt(System.getProperty("timeInterval")),
		// TimeUnit.MINUTES);

	}

}

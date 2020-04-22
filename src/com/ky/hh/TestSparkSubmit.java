package com.ky.hh;

import static com.ky.util.AccommUtil.PROP_CONFIG;
import static com.ky.util.StringUtils.nvl;
import static com.ky.db.OracleConn.insertSql;
import static com.ky.db.OracleConn.updateSql;
import static com.ky.util.MapUtils.create;
import static com.ky.util.StringUtils.isNotBlank;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

import com.ky.db.OracleConn;
import com.ky.util.AccommUtil;

public class TestSparkSubmit {
	
	public static String rootDir = AccommUtil.getRuntimeRootDir();
	/**
	 * hive配置信息
	 */
	private static final String HIVE_SITE_PATH = PROP_CONFIG.getString("hive.site.path");
	/**
	 * 关系表
	 */
	private static final String RELATION_VIEW = PROP_CONFIG.getString("relation.view");
	private static final String EXECUTOR_INSTANCES = PROP_CONFIG.getString("spark.executor.instances");
	private static final String EXECUTOR_MEMORY = PROP_CONFIG.getString("spark.executor.memory");
	private static final String EXECUTOR_CORES = PROP_CONFIG.getString("spark.executor.cores");
	private static final String DEFAULT_PARALLELISM = PROP_CONFIG.getString("spark.default.parallelism");
	
	private static ExecutorService executorWriteHbaseService;
	
	private static final String LOG_TABLE = "spark_hh_log";
	
	private static int finished_thread = 0;
	private static int total_thread = 0;
	
	private static String VER = "2";

	public static void main(String[] args) {
		List<Map<String, String>> cfgs = OracleConn.list(RELATION_VIEW, null, "*");
		
		if(CollectionUtils.isEmpty(cfgs)) {
			return;
		}
		total_thread = cfgs.size();
		executorWriteHbaseService = Executors.newScheduledThreadPool(total_thread);
		for(Map<String, String> cfg : cfgs) {
			String[] appArgs = new String[] { cfg.get("hive_tbname"),
					cfg.get("rowkey"), cfg.get("hbase_tbname"),
					cfg.get("trsdr_sjly"), cfg.get("hive_columns"),
					nvl(cfg.get("hbase_columns")),
					cfg.get("hive_columns_cjsj"),
					nvl(cfg.get("hive_columns_cjsj_lasttime")),
					nvl(cfg.get("hive_columns_cjsj_add_time")),
					nvl(cfg.get("sfzhm_columns")), cfg.get("id"),
					nvl(cfg.get("time_step_num")),
					cfg.get("hive_columns_cjsj_type") };
			executorWriteHbaseService.execute(() -> submitSparkApp(appArgs));
		}
		
//		String[] params = { "hz_bzk.dwb_shsj_t_unit",
//				"cydwbm,systemid,bzk_rksj",
//				"fk_graphs:dwb_shsj_t_unit_test_ljie",
//				"社会数据-公司信息"};
	}
	
	public static void submitSparkApp(String[] params) {
		String uuid = UUID.randomUUID().toString();
		Object[] newPar = ArrayUtils.add(params, uuid);
//		System.out.println("params: " + Arrays.asList(params).stream().collect(Collectors.joining("\n")));
		SparkLauncher sparkLLanuchser = new SparkLauncher().setAppName("copyData~" + uuid)
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setMainClass("com.ky.hh.TestSparkHH" + VER)
//                .setConf(SparkLauncher.DRIVER_MEMORY, "8g")
                .setConf("spark.driver.cores", "4")
//                .setConf("spark.driver.maxResultSize", "4g")
                .setConf(SparkLauncher.EXECUTOR_MEMORY, EXECUTOR_MEMORY)
                .setConf("spark.executor.instances", EXECUTOR_INSTANCES)
                .setConf(SparkLauncher.EXECUTOR_CORES, EXECUTOR_CORES)
//                .setConf("spark.task.cpus", "2")
//                .setConf("spark.shuffle.io.numConnectionsPerPeer", "5") //设置节点之间 shuffle拉取数据的连接数量
//                .setConf("spark.dynamicAllocation.enabled", "true")
//                .setConf("spark.shuffle.service.enabled", "true")
                .setConf("spark.default.parallelism", DEFAULT_PARALLELISM)
//                .setConf("spark.memory.fraction", "0.1")
                .setConf("spark.yarn.maxAppAttempts", "1")
//                .setConf("spark.speculation", "true")
//                .setConf("spark.locality.wait", "0")
//                .setConf("spark.network.timeout", "60s")
//                .setConf("spark.executor.heartbeatInterval", "100s") //设置心跳时间
//                .setConf("spark.executor.extraJavaOptions", JVM)
//                .setConf("spark.shuffle.file.buffer", "10m")
//                .setConf("spark.reducer.maxSizeInFlight", "96m")
//                .setConf("spark.shuffle.consolidateFiles", "true")
                  .setConf("spark.sql.shuffle.partitions", "1000")
                .setConf("spark.files", HIVE_SITE_PATH)
                .setAppResource(rootDir + "/spark_hh"+ VER + ".jar")
                .addAppArgs(Arrays.asList(newPar).toArray(new String[newPar.length]));
		Listener appListener = new SparkAppHandle.Listener() {
			@Override
			public void stateChanged(SparkAppHandle appHandle) {
				System.out.println("stateChanged  任务 id ="
						+ appHandle.getAppId() + " 当前任务状态 = "
						+ appHandle.getState().toString());
				if(State.FAILED.equals(appHandle.getState())) {
					updateSql("update " + LOG_TABLE
							+ " set IS_SUCCESS='0',LAST_UPDATE_TIME=sysdate where appid=?",
							appHandle.getAppId());
				}
			}

			@Override
			public void infoChanged(SparkAppHandle appHandle) {
				System.out.println("infoChanged 任务 id =" + appHandle.getAppId()
						+ " 当前任务状态 = " + appHandle.getState().toString());
			}
		};
		try {
			boolean isFirst = true;
			//启动spark 任务 
			SparkAppHandle appHandler = sparkLLanuchser.startApplication(appListener);
			while (!State.FINISHED.equals(appHandler.getState())
					&& !State.FAILED.equals(appHandler.getState())
					&& !State.KILLED.equals(appHandler.getState())) {
//				System.out.println("表名: " + params[3] + "isFirst: " + isFirst + " ~ appid: " + appHandler.getAppId());
				if(isFirst && isNotBlank(appHandler.getAppId())) {
					insertSql(
							LOG_TABLE,
							create("ID", uuid, "RELAID", params[10],
									"HIVE_TBNAME", params[0], "START_TIME",
									new Date(), "APP_URL",
									"http://tbds-10-166-114-22:8084/proxy/"
											+ appHandler.getAppId(), "APPID", appHandler.getAppId()));
					isFirst = false;
				}
				System.out.println("spark 任务 id ="+ appHandler.getAppId() +" 当前任务状态 = "+appHandler.getState().toString());
				System.out.println(params[3] + " = http://tbds-10-166-114-22:8084/proxy/"+appHandler.getAppId());
				Thread.currentThread().sleep(5000);
			}
			if (State.FAILED.equals(appHandler.getState())
					|| State.KILLED.equals(appHandler.getState())) {
				updateSql("update " + LOG_TABLE
						+ " set IS_SUCCESS='0',LAST_UPDATE_TIME=sysdate where id=?", uuid);
				finished_thread++;
			} else if(State.FINISHED.equals(appHandler.getState())) {
				finished_thread++;
			}
			
			if(finished_thread >= total_thread) {
				executorWriteHbaseService.shutdown();
				System.exit(1);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

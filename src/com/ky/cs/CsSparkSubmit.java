package com.ky.cs;

import static com.ky.util.AccommUtil.PROP_CONFIG;

import java.util.UUID;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

import com.ky.util.AccommUtil;

public class CsSparkSubmit {
	
	public static String rootDir = AccommUtil.getRuntimeRootDir();
	
	private static final String EXECUTOR_INSTANCES = PROP_CONFIG.getString("spark.executor.instances");
	private static final String EXECUTOR_MEMORY = PROP_CONFIG.getString("spark.executor.memory");
	private static final String EXECUTOR_CORES = PROP_CONFIG.getString("spark.executor.cores");
	private static final String DEFAULT_PARALLELISM = PROP_CONFIG.getString("spark.default.parallelism");
	
	public static void main(String[] args) {
		submitSparkApp();
	}
	
	public static void submitSparkApp() {
		String uuid = UUID.randomUUID().toString();
		SparkLauncher sparkLLanuchser = new SparkLauncher().setAppName("csData~" + uuid)
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setMainClass("com.ky.cs.CsSpark")
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
                .setAppResource(rootDir + "/spark_cs.jar");
		try {
			SparkAppHandle appHandler = sparkLLanuchser.startApplication();
			while (!"FINISHED".equals(appHandler.getState().toString())
					&& !State.FAILED.equals(appHandler.getState())) {
				System.out.println("spark 任务 id =" + appHandler.getAppId() + " 当前任务状态 = "
						+ appHandler.getState().toString());
				Thread.currentThread().sleep(3000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

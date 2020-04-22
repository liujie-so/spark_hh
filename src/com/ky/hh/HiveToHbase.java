package com.ky.hh;

import static com.ky.util.StringUtils.nvl;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HRegionLocator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import com.google.common.collect.Lists;
import com.ky.db.HbaseConn;


public class HiveToHbase {
	
	private static final Log log = LogFactory.getLog(HiveToHbase.class);
	
	private static final String HIVE_TABLE = "dwb_hn_zyryxx";
	
	private static final String HTABLE_COLUMN_FAMILY = "cf1";

	public static void main(String[] args) throws Exception {
		log.info("------初始化sparkcontext: " + args);
		Class<?>[] classes = { ImmutableBytesWritable.class, KeyValue.class,
				Put.class, ImmutableBytesWritable.Comparator.class };
		SparkConf conf = new SparkConf()
				.setAppName("copyData")
				.setMaster("yarn")
				.set("spark.serializer",
						"org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(classes);
		
		SparkSession spark = SparkSession.builder().config(conf)
				.enableHiveSupport().getOrCreate();
		
		log.info("使用 sql: " + HIVE_TABLE);
		
		spark.sql("use hz_bzk");
		
		log.info("执行 sql: " + HIVE_TABLE);
		
		Dataset<Row> dataset = spark.sql("select * from " + HIVE_TABLE);
		
		String tableName = "fk_graphs:"+ HIVE_TABLE +"_test_lj";
		TableName tabName = TableName.valueOf(tableName);
		HbaseConn.createTable(tabName, HTABLE_COLUMN_FAMILY);
		
		log.info("总量 count: " + dataset.count());
		
		JavaPairRDD<ImmutableBytesWritable, KeyValue> flatMapToPair;
		flatMapToPair = dataset.javaRDD().flatMapToPair(new PairFlatMapFunction<Row, ImmutableBytesWritable, KeyValue>() {
			private static final long serialVersionUID = 9109101926803068008L;

			@Override
			public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Row row)
					throws Exception {
				List<Tuple2<ImmutableBytesWritable, KeyValue>> kvs = Lists.newArrayList();
				String uuid = UUID.randomUUID().toString();
				kvs.add(new Tuple2<>(new ImmutableBytesWritable(uuid.getBytes()), 
						new KeyValue(uuid.getBytes(), HTABLE_COLUMN_FAMILY.getBytes(), 
								"gmsfhm".getBytes(), Bytes.toBytes(nvl(row.<String>getAs("gmsfhm"))))));
				kvs.add(new Tuple2<>(new ImmutableBytesWritable(uuid.getBytes()), 
						new KeyValue(uuid.getBytes(), HTABLE_COLUMN_FAMILY.getBytes(), 
								"rybh".getBytes(), Bytes.toBytes(nvl(row.<String>getAs("rybh"))))));
				
				return kvs.iterator();
			}
		}).sortByKey();
		
		Configuration configuration = HbaseConn.configuration;
		configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		configuration.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
				"/project/zdk_yunnan/fk/my_tmp");
		
		Connection conn = HbaseConn.createConn();
		Table table = conn.getTable(tabName);
		
		Job job = Job.getInstance(configuration);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		
		HRegionLocator regionLocator = new HRegionLocator(tabName,
				(ClusterConnection) conn);
		
		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
		
		String hdfsOutputDir = "/project/zdk_yunnan/fk/my_hfile/"
				+ System.currentTimeMillis();
		
		flatMapToPair.saveAsNewAPIHadoopFile(hdfsOutputDir,
				ImmutableBytesWritable.class, KeyValue.class,
				HFileOutputFormat2.class, configuration);
		
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
		
		loader.doBulkLoad(new Path(hdfsOutputDir), conn.getAdmin(), table,
				regionLocator);
		
		spark.stop();
		spark.close();
	}

}

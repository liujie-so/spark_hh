package com.ky.cs;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.ky.db.HbaseConn;

public class CsSpark {
	private static final Log log = LogFactory.getLog(CsSpark.class);
	
	private static final String START_KEY = "20200323";
	private static final String END_KEY = "20200324~";
	
	private static final byte[] CF_BYTES = toBytes("cf1");
	private static final byte[] ZJHM_BYTES = toBytes("zjhm");
	
	public static void main(String[] args) throws Exception {
		log.info("------初始化sparkcontext------");
		SparkConf conf = new SparkConf().setAppName("csData").setMaster("yarn")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		Scan scan = new Scan();
		scan.setStartRow(START_KEY.getBytes());
		scan.setStopRow(END_KEY.getBytes());
		Configuration hbConf = HBaseConfiguration.create(HbaseConn.configuration);
		String hbTable = "fk_graphs:t_date_ldzs";
		// 执行
		JavaRDD<String> ldzs = process(jsc, hbConf, hbTable, scan);
		hbTable = "fk_graphs:t_date_kydp";
		JavaRDD<String> kydp = process(jsc, hbConf, hbTable, scan);
		hbTable = "fk_graphs:t_date_wbsw";
		JavaRDD<String> wbsw = process(jsc, hbConf, hbTable, scan);
		hbTable = "fk_graphs:t_date_tlsp";
		JavaRDD<String> tlsp = process(jsc, hbConf, hbTable, scan);
		hbTable = "fk_graphs:t_date_tlyp";
		JavaRDD<String> tlyp = process(jsc, hbConf, hbTable, scan);
		hbTable = "fk_graphs:t_date_mhlg";
		JavaRDD<String> mhlg = process(jsc, hbConf, hbTable, scan);
		hbTable = "fk_graphs:t_date_mhdp";
		JavaRDD<String> mhdp = process(jsc, hbConf, hbTable, scan);
		JavaRDD<String> distinct = ldzs.union(kydp).union(wbsw).union(tlsp).union(tlyp).union(mhlg)
				.union(mhdp).distinct().coalesce(1);
		
//		System.out.println("----------------------count: " + distinct.count());
		distinct.saveAsTextFile("/project/zdk_yunnan/fk/my_csv/trace_zjhms");
	}
	
	public static JavaRDD<String> process(JavaSparkContext jsc, Configuration hbConf, String hbTable, Scan scan)
			throws IOException {
		hbConf.set(TableInputFormat.INPUT_TABLE, hbTable);
		String scanString = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
		hbConf.set(TableInputFormat.SCAN, scanString);
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc.newAPIHadoopRDD(hbConf,
				TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		// f._2.getValue(toBytes("cf1"), toBytes("zjhm"))
		return hbaseRDD.map(f -> Bytes.toString(f._2.getValue(CF_BYTES, ZJHM_BYTES))).distinct();
	}
}

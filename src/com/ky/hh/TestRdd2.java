package com.ky.hh;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;

public class TestRdd2 {
	
	private static final byte[] CF_BYTES = toBytes("cf1");
	private static final byte[] XM_BYTES = toBytes("xm");
	private static final byte[] AGE_BYTES = toBytes("age");
	private static final byte[] SEX_BYTES = toBytes("sex");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("testRdd").setMaster("local").set("spark.testing.memory", "2147480000");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark
				.sparkContext());
		
		List<Tuple2<String, Integer>> list = Arrays.asList(
				new Tuple2<String, Integer>("zhangsan", 10),
				new Tuple2<String, Integer>("lishi", 15),
				new Tuple2<String, Integer>("zhangsan", 10),
				new Tuple2<String, Integer>("zhangsan", 10));
		
		JavaRDD<Tuple2<String, Integer>> rdd = jsc.parallelize(list);
		JavaRDD<Tuple2<String, Integer>> rdd2 = rdd.distinct();
		
		rdd2.foreach(System.out::println);
	
	}

}

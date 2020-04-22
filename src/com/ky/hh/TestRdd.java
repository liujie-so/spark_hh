package com.ky.hh;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;

public class TestRdd {
	
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
		
		Tuple2<String, Tuple3<String, String, String>> t1;
		t1 = new Tuple2<String, Tuple3<String, String, String>>("row1",
				new Tuple3<String, String, String>("row1", "age", "18"));
		Tuple2<String, Tuple3<String, String, String>> t2;
		t2 = new Tuple2<String, Tuple3<String, String, String>>("row1",
				new Tuple3<String, String, String>("row1", "sex", "famale"));
		Tuple2<String, Tuple3<String, String, String>> t3;
		t3 = new Tuple2<String, Tuple3<String, String, String>>("row1",
				new Tuple3<String, String, String>("row1", "xm", "zhangsan"));
		Tuple2<String, Tuple3<String, String, String>> t4;
		t4 = new Tuple2<String, Tuple3<String, String, String>>("row2",
				new Tuple3<String, String, String>("row2", "age", "~20"));
		Tuple2<String, Tuple3<String, String, String>> t5;
		t5 = new Tuple2<String, Tuple3<String, String, String>>("row2",
				new Tuple3<String, String, String>("row2", "xm", "lishi"));
		Tuple2<String, Tuple3<String, String, String>> t6;
		t6 = new Tuple2<String, Tuple3<String, String, String>>("row2",
				new Tuple3<String, String, String>("row2", "age", "20~"));

		List<Tuple2<String, Tuple3<String, String, String>>> list = Arrays.asList(t1, t2, t3, t4, t5, t6);
		
		JavaPairRDD<String, Tuple3<String, String, String>> rdd = jsc.parallelizePairs(list);
//		JavaPairRDD<String, Iterable<Tuple2<String, Tuple3<String, String, String>>>> groupBy;
		JavaPairRDD<String, Tuple3<String, String, String>> reduceByKey = rdd.reduceByKey((v1, v2) -> v2);
		reduceByKey.foreach(System.out::println);
		
//		JavaPairRDD<String, ArrayList<Tuple3<String, String, String>>> combineByKey = rdd
//				.combineByKey(v1 -> Lists.newArrayList(v1),
//						(v1, v2) -> {
//							if (v1.stream().filter(f -> f._2().equals(v2._2()))
//									.count() == 0) {
//								v1.add(v2);
//							}
//							return v1;
//						}, (v1, v2) -> {
//							v1.addAll(v2);
//							return v1;
//						}).sortByKey();
//		System.out.println(combineByKey.collect());
	}

}

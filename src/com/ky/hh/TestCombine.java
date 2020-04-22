package com.ky.hh;

import static com.ky.util.StringUtils.nvl;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import com.google.common.collect.Lists;
import com.ky.util.DateUtils;

import scala.Tuple2;
import scala.Tuple3;

public class TestCombine {
	
	private static final Log log = LogFactory.getLog(TestCombine.class);
	
	private static final String HTABLE_COLUMN_FAMILY = "cf1";
	private static final byte[] CF_BYTES = toBytes(HTABLE_COLUMN_FAMILY);

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("testCombine").setMaster(
				"yarn");

		SparkSession spark = SparkSession.builder().config(conf)
				.enableHiveSupport().getOrCreate();

		Dataset<Row> dataset = spark.sql("select * from hz_bzk.dwb_shsj_t_unit limit 10");
		
		JavaPairRDD<ImmutableBytesWritable, KeyValue> flatMapToPair;
		flatMapToPair = dataset.javaRDD().flatMapToPair(new PairFlatMapFunction<Row, ImmutableBytesWritable, KeyValue>() {
			private static final long serialVersionUID = 9109101926803068008L;

			@Override
			public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Row row)
					throws Exception {
				
				List<StructField> fns = Arrays.asList(row.schema().fields());
				// 这样排序会导致ROW中数据错位
//				fns.sort((e1, e2) -> e1.name().compareTo(e2.name()));
				
				String rowkey = convert(row, "cydwbm", "string", "null") + "_"
						+ convert(row, "bzk_rksj", "timestamp", "null");
				
				byte[] rk = rowkey.getBytes();
				
				List<Tuple3<String, ImmutableBytesWritable, KeyValue>> ls;
				ls = fns.stream()
						.map(f -> new Tuple3<>(f.name(),
								new ImmutableBytesWritable(rk), new KeyValue(rk, CF_BYTES,
										toBytes(f.name()), toBytes(convert(row, f)))))
						.collect(Collectors.toList());
				ls.sort((e1, e2) -> e1._1().compareTo(e2._1()));
//				
				List<Tuple2<ImmutableBytesWritable, KeyValue>> kvs;
				kvs = ls.stream().map(f -> new Tuple2<>(f._2(), f._3()))
						.collect(Collectors.toList());
				// 累加数量
				return kvs.iterator();
			}
		});
		
		System.out.println("打印 combineByKey: " + flatMapToPair.collect());
		
		JavaPairRDD<ImmutableBytesWritable, ArrayList<KeyValue>> combineByKey = flatMapToPair.combineByKey(v1 -> Lists.newArrayList(v1), (v1, v2) -> {
			if(v1.stream().filter(f -> CellUtil.cloneQualifier(f).equals(CellUtil.cloneQualifier(v2))).count() == 0) {
				v1.add(v2);
			}
			return v1;
		}, (v1, v2) -> {
			v1.addAll(v2);
			return v1;
		});
		System.out.println("打印 combineByKey: " + combineByKey.collect());
		
		spark.stop();
		spark.close();
	}
	
	private static String convert(Row row, String fieldName, String dataType,
			String defaultValue) {
		if("timestamp".equals(dataType)) {
			Timestamp timestamp = row.getAs(fieldName);
			if(timestamp == null) {
				return defaultValue;
			}
			return DateUtils.format(timestamp, DateUtils.innerPatterns[7]);
		} else {
			return nvl(row.<String>getAs(fieldName), defaultValue);
		}
	}
	
	private static String convert(Row row, String fieldName, String dataType) {
		return convert(row, fieldName, dataType, "");
	}
	
	private static String convert(Row row, StructField sf) {
		return convert(row, sf.name(), sf.dataType().typeName());
	}

}

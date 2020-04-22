package com.ky.hh;

import static com.ky.util.StringUtils.nvl;
import static com.ky.util.StringUtils.isBlank;
import static com.ky.util.StringUtils.isNotBlank;
import static com.ky.util.StringUtils.EMPTY;
import static com.ky.util.StringUtils.trim;
import static com.ky.util.StringUtils.substringBefore;
import static com.ky.util.StringUtils.replaceEach;
import static com.ky.util.StringUtils.endsWithIgnoreCase;
import static com.ky.util.StringUtils.stringToList;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static com.ky.util.AccommUtil.PROP_CONFIG;
import static com.ky.db.OracleConn.updateSql;
import static com.ky.db.OracleConn.insertSql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

import com.ky.db.HbaseConn;
import com.ky.db.OracleConn;
import com.ky.util.DateUtils;
import com.ky.util.IdCards;
import com.ky.util.MapUtils;
import com.ky.util.StringUtils;

/**
 * hive数据取样
 * @author liu.jie
 *
 */
public class TestSparkHH3 {
	
	private static final Log log = LogFactory.getLog(TestSparkHH3.class);
	
	private static final String HTABLE_COLUMN_FAMILY = "cf1";
	private static final byte[] CF_BYTES = toBytes(HTABLE_COLUMN_FAMILY);
	
	private static final String[] searchs = { "/", "-", " ", ":" };
    private static final String[] replaces = { "", "", "", "" };
    
    /**
     * 配置的“身份证号码”列名称集
     */
	private static String[] sfzhms;
	
	private static final String LOG_TABLE = "spark_hh_log";
	/**
	 * task数量
	 */
	private static final Integer TASK_NUM = Integer.valueOf(PROP_CONFIG.getString("spark.task.num"));
	/**
	 * 关系表
	 */
	private static final String RELATION_TABLE = PROP_CONFIG.getString("relation.table");

	public static void main(String[] args) throws Exception {
		log.info("------初始化sparkcontext------");
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
		
		LongAccumulator myAcc = spark.sparkContext().longAccumulator("myAcc");
		
		String hiveTable = args[0];
		String rowkey = args[1];
		
		String hbaseTable = args[2];
		
//		String trsdrSjly = args[3];
		String hiveColumns = args[4];
//		String hbaseColumns = args[5];
		String incrementColumn = args[6];
		String incrementLastTime = args[7];
//		String incrementAddTime = args[8];
		
		String sfzhmColumns = args[9];
		if(isNotBlank(trim(sfzhmColumns))) {
			sfzhms = stringToList(sfzhmColumns).toArray(
					ArrayUtils.EMPTY_STRING_ARRAY);
		}
		String timeStepNum = args[11];
		String hiveColumnDataType = args[12];
		String wherePart = "";
		String lastTime = "";
		if(isNotBlank(trim(incrementLastTime))) {
			lastTime = incrementLastTime;
			Date lt = DateUtils.parseFullDate(incrementLastTime);
			if("string".equals(hiveColumnDataType)) {
				wherePart = " where " + incrementColumn + " >= '" + incrementLastTime + "' ";
			} else {
				wherePart = " where " + incrementColumn + " >= '"
						+ DateUtils.format(lt, DateUtils.innerPatterns[0]) + "' ";
			}
			int step = 0;
			// 默认步长为0：表示没有时间范围限制，非0：表示按时间范围检索数据
			if(isNotBlank(timeStepNum)) {
				step = Integer.valueOf(timeStepNum);
				Date am = DateUtils.addDays(lt, step);
				lastTime = DateUtils.benchmark(am);
				if("string".equals(hiveColumnDataType)) {
					wherePart += " and " + incrementColumn + " < '" + lastTime + "'";
				} else {
					wherePart += " and " + incrementColumn + " < '"
							+ DateUtils.format(am, DateUtils.innerPatterns[0]) + "'";
 				}
			}
		}
		
		String relaid = args[10];
		String logid = args[args.length - 1];
		
		log.info("执行 sql: " + hiveTable);
		
		Dataset<Row> dataset = spark.sql("select " + hiveColumns + " from "
				+ hiveTable + wherePart);
		
//		log.info("总量 count: " + dataset.count());
		
		Stream.of(dataset.columns()).forEach(System.out::println);
		
		TableName tabName = TableName.valueOf(hbaseTable);
		HbaseConn.createTable(tabName, HTABLE_COLUMN_FAMILY);
		
		if(StringUtils.isAllUpperCase(dataset.columns()[0])) {
			rowkey = rowkey.toUpperCase();
			incrementColumn = incrementColumn.toUpperCase();
		}
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		
		Broadcast<List<String>> rowkeys = jsc.broadcast(StringUtils
				.stringToList(rowkey).stream().map(StringUtils::trim)
				.collect(Collectors.toList()));
		
		log.info("打印 count: " + dataset.javaRDD().count());
		
		JavaRDD<String> sample = dataset.javaRDD().map(row -> {
			String rk = rowkeys.getValue().stream().map(f -> convert(row, f, "null"))
					.collect(Collectors.joining("_"));
			return rk;
		}).distinct().sample(false, 0.001);
		
		
		Connection conn = OracleConn.getOracleConn();
		
		sample.collect().forEach(f -> {
			try {
				PreparedStatement pstm = conn.prepareStatement("insert into spark_sample_tmp(rowkey) values(?)");
				pstm.setString(1, f);
				pstm.executeUpdate();
				pstm.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		if (conn != null && !conn.isClosed()) {
			conn.close();
		}
		
		spark.stop();
		spark.close();
	}
	
	private static String convert(Row row, String fieldName, String defaultValue) {
		Object value = row.getAs(fieldName);
		if(isBlank(value)) {
			return defaultValue;
		}
		if(value instanceof Timestamp) {
			return DateUtils.benchmark((Timestamp) value);
		} else if(value instanceof Date) {
			return DateUtils.benchmarkOfDate((Date) value);
		}else {
			if (endsWithIgnoreCase(fieldName, "sj")
					|| endsWithIgnoreCase(fieldName, "rq")) {
				value = formatDateString(trim(value.toString()));
			} else if (ArrayUtils.isNotEmpty(sfzhms)
					&& ArrayUtils.contains(sfzhms, fieldName)) {
				value = IdCards.toEighteen(value.toString());
			}
			return nvl(trim(value.toString()), defaultValue);
		}
	}
	
	private static String convert(Put put, String fieldName, String defaultValue) {
		List<Cell> list = put.get(CF_BYTES, toBytes(fieldName));
		if(CollectionUtils.isEmpty(list)) {
			return defaultValue;
		}
		String value = new String(CellUtil.cloneValue(list.get(0)));
		if(isBlank(value)) {
			return defaultValue;
		}
		
		return value;
	}
	
	private static String convert(Row row, String fieldName) {
		return convert(row, fieldName, "");
	}
	
	/**
	 * 对日期字符串进行处理
	 * @param str
	 * @return
	 */
	private static String formatDateString(String str){
		if(isNotBlank(str)){
			String re = replaceEach(str, searchs, replaces);
			return substringBefore(re, ".");
		}
		return EMPTY;
	}

}

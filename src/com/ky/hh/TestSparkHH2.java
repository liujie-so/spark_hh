package com.ky.hh;

import static com.ky.util.StringUtils.nvl;
import static com.ky.util.StringUtils.isBlank;
import static com.ky.util.StringUtils.isNotBlank;
import static com.ky.util.StringUtils.EMPTY;
import static com.ky.util.StringUtils.trim;
import static com.ky.util.StringUtils.substringBefore;
import static com.ky.util.StringUtils.endsWithIgnoreCase;
import static com.ky.util.StringUtils.stringToList;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static com.ky.util.AccommUtil.PROP_CONFIG;
import static com.ky.db.OracleConn.updateSql;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

import com.ky.db.HbaseConn;
import com.ky.util.DateUtils;
import com.ky.util.IdCards;
import com.ky.util.StringUtils;

/**
 * hive数据抽取
 * @author liu.jie
 *
 */
public class TestSparkHH2 {
	
	private static final Log log = LogFactory.getLog(TestSparkHH2.class);
	
	private static String HTABLE_COLUMN_FAMILY = "cf1";
	private static byte[] CF_BYTES = toBytes(HTABLE_COLUMN_FAMILY);
	
//	private static final String[] searchs = { "/", "-", " ", ":" };
//	private static final String[] replaces = { "", "", "", "" };
    
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
	/** hbase表信息是否大写*/
	private static boolean isUpperCase = false;
	static {
		isUpperCase = "1".equals(PROP_CONFIG.getString("hbase.upper.case"));
		if(isUpperCase) {
			HTABLE_COLUMN_FAMILY = "CF1";
			CF_BYTES = toBytes(HTABLE_COLUMN_FAMILY);
		}
	}

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
		if(isUpperCase) {
			hbaseTable = hbaseTable.toUpperCase();
		}
		
//		String trsdrSjly = args[3];
		String hiveColumns = args[4];
//		String hbaseColumns = args[5];
		String incrementColumn = args[6];
		String incrementLastTime = args[7];
//		String incrementAddTime = args[8];
		
		String sfzhmColumns = args[9];
		if(isNotBlank(trim(sfzhmColumns))) {
			sfzhms = stringToList(sfzhmColumns).toArray(ArrayUtils.EMPTY_STRING_ARRAY);
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
						+ DateUtils.format(lt, "yyyy-MM-dd HH:mm:ss") + "' ";
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
							+ DateUtils.format(am, "yyyy-MM-dd HH:mm:ss") + "'";
 				}
			}
		}
		
		String relaid = args[10];
		String logid = args[args.length - 1];
		
		log.info("执行 sql: " + hiveTable);
		
		Dataset<Row> dataset = spark.sql("select " + hiveColumns + " from " + hiveTable + wherePart);
		
//		log.info("总量 count: " + dataset.count());
		
//		Stream.of(dataset.columns()).forEach(System.out::println);
		
		TableName tabName = TableName.valueOf(hbaseTable);
		HbaseConn.createTable(tabName, HTABLE_COLUMN_FAMILY);
		
		if(StringUtils.isAllUpperCase(dataset.columns()[0])) {
			rowkey = rowkey.toUpperCase();
			incrementColumn = incrementColumn.toUpperCase();
		}
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		
		Broadcast<String> incColumn = jsc.broadcast(incrementColumn);
		
		Broadcast<List<String>> rowkeys = jsc.broadcast(StringUtils
				.stringToList(rowkey).stream().map(StringUtils::trim)
				.collect(Collectors.toList()));
		
		
		JavaPairRDD<String, Tuple2<ImmutableBytesWritable, Put>> mapToPair;
		mapToPair = dataset.javaRDD()
				.mapToPair(new PairFunction<Row, String, Tuple2<ImmutableBytesWritable, Put>>() {
					private static final long serialVersionUID = -2214409427521929931L;

					@Override
					public Tuple2<String, Tuple2<ImmutableBytesWritable, Put>> call(Row row) throws Exception {
						String rowkey = rowkeys.getValue().stream().map(f -> convert(row, f, "null"))
								.collect(Collectors.joining("_"));
						List<String> fns = Arrays.asList(row.schema().fieldNames());
						fns.sort((v1, v2) -> v1.compareTo(v2));

						byte[] rk = rowkey.getBytes();
						ImmutableBytesWritable writable = new ImmutableBytesWritable(rk);

						Put put = new Put(rk);
						fns.stream().forEach(
								f -> put.addColumn(CF_BYTES,
										toBytes(isUpperCase ? f.toUpperCase() : f.toLowerCase()),
										toBytes(convert(row, f))));
						return new Tuple2<>(rowkey, new Tuple2<>(writable, put));
					}
				}).reduceByKey((v1, v2) -> v2, TASK_NUM).cache();
		
		JavaPairRDD<ImmutableBytesWritable, Put> flatMapToPair;
		flatMapToPair = mapToPair.mapToPair(t -> t._2).sortByKey();
		
		Tuple2<String, Tuple2<ImmutableBytesWritable, Put>> maxTime;
		if (!mapToPair.isEmpty()) {
			maxTime = mapToPair.reduce((t1, t2) -> {
				String v1 = convert(t1._2._2, incColumn.value(), "0");
				String v2 = convert(t2._2._2, incColumn.value(), "0");

				if (!NumberUtils.isNumber(v1)) {
					v1 = "0";
				}
				if (!NumberUtils.isNumber(v2)) {
					v2 = "0";
				}
				myAcc.add(1L);
				if (Long.valueOf(v1) >= Long.valueOf(v2)) {
					return t1;
				} else {
					return t2;
				}
			});
			lastTime = convert(maxTime._2._2, incrementColumn, "");
		}
		
		Configuration hdpConf = HBaseConfiguration.create(HbaseConn.configuration);
		hdpConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable);
		
		Job job = Job.getInstance(hdpConf);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		flatMapToPair.saveAsNewAPIHadoopDataset(job.getConfiguration());
		
		updateSql("update " + LOG_TABLE
				+ " set ROW_NUMS=?,END_TIME=sysdate,IS_SUCCESS='1',INCREMENT_TIME=? where id=?",
				myAcc.value(), lastTime, logid);
		updateSql("update " + RELATION_TABLE
				+ " set hive_columns_cjsj_lasttime=? where id=?", lastTime, relaid);
		
		log.info("总量 value: " + myAcc.value() + "\n");
		
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
			if (endsWithIgnoreCase(fieldName, "sj") || endsWithIgnoreCase(fieldName, "rq")) {
				value = formatDateString(trim(value.toString()));
			} else if (ArrayUtils.isNotEmpty(sfzhms) && ArrayUtils.contains(sfzhms, fieldName)) {
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
			String re = substringBefore(str, ".");
			Date d = DateUtils.parseFullDate(re);
			if(d == null) {
				return str;
			}
			return DateUtils.benchmark(d);
		}
		return EMPTY;
	}

}

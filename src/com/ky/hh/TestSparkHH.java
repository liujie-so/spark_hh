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

import java.util.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
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
public class TestSparkHH {
	
	private static final Log log = LogFactory.getLog(TestSparkHH.class);
	
	private static final String HTABLE_COLUMN_FAMILY = "cf1";
	private static final byte[] CF_BYTES = toBytes(HTABLE_COLUMN_FAMILY);
	
	private static final String HDFS_TMP_DIR = PROP_CONFIG.getString("hdfs.tmp.dir");
	private static final String HDFS_OUTPUT_DIR = PROP_CONFIG.getString("hdfs.output.dir");
	
	private static final String[] searchs = { "/", "-", " ", ":" };
    private static final String[] replaces = { "", "", "", "" };
    
    /**
     * 配置的“身份证号码”列名称集
     */
	private static String[] sfzhms;
	
	private static final String LOG_TABLE = "spark_hh_log";
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
		if(isNotBlank(trim(incrementLastTime))) {
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
				if("string".equals(hiveColumnDataType)) {
					wherePart += " and " + incrementColumn + " < '" + DateUtils.benchmark(am) + "'";
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
		
		Broadcast<String> incColumn = jsc.broadcast(incrementColumn);
		
		Broadcast<List<String>> rowkeys = jsc.broadcast(StringUtils
				.stringToList(rowkey).stream().map(StringUtils::trim)
				.collect(Collectors.toList()));
		
		JavaPairRDD<String, List<Tuple2<ImmutableBytesWritable, KeyValue>>> mapToPair;
		mapToPair = dataset.javaRDD()
				.mapToPair(new PairFunction<Row, String, List<Tuple2<ImmutableBytesWritable, KeyValue>>>() {
			private static final long serialVersionUID = 7185206361777637944L;

			@Override
			public Tuple2<String, List<Tuple2<ImmutableBytesWritable, KeyValue>>> call(Row row) throws Exception {
				String rowkey = rowkeys.getValue().stream()
						.map(f -> convert(row, f, "null"))
						.collect(Collectors.joining("_"));
				List<String> fns = Arrays.asList(row.schema().fieldNames());
				fns.sort((v1, v2) -> v1.compareTo(v2));
				
				byte[] rk = rowkey.getBytes();
				ImmutableBytesWritable writable = new ImmutableBytesWritable(rk);
				
				List<Tuple2<ImmutableBytesWritable, KeyValue>> ls;
				ls = fns.parallelStream()
						.map(f -> new Tuple2<>(
								writable, new KeyValue(rk, CF_BYTES,
										toBytes(f.toLowerCase()), toBytes(convert(row, f)))))
						.collect(Collectors.toList());
				
				return new Tuple2<>(rowkey, ls);
			}
		}).reduceByKey((v1, v2) -> v2, 1000).cache();
		
		JavaPairRDD<ImmutableBytesWritable, KeyValue> flatMapToPair;
		flatMapToPair = mapToPair.flatMapToPair(t -> t._2.iterator())
				.sortByKey();
		
		Tuple2<String, List<Tuple2<ImmutableBytesWritable, KeyValue>>> maxTime;
		String lastTime = incrementLastTime;
		if(!mapToPair.isEmpty()) {
			maxTime = mapToPair.reduce(new Function2<Tuple2<String,List<Tuple2<ImmutableBytesWritable, KeyValue>>>, 
					Tuple2<String,List<Tuple2<ImmutableBytesWritable, KeyValue>>>, 
					Tuple2<String,List<Tuple2<ImmutableBytesWritable, KeyValue>>>>() {
				private static final long serialVersionUID = -1785038692694842753L;
				
				@Override
				public Tuple2<String, List<Tuple2<ImmutableBytesWritable, KeyValue>>> call(
						Tuple2<String, List<Tuple2<ImmutableBytesWritable, KeyValue>>> t1,
						Tuple2<String, List<Tuple2<ImmutableBytesWritable, KeyValue>>> t2) throws Exception {
					String v1 = convert(t1._2, incColumn.value(), "0");
					String v2 = convert(t2._2, incColumn.value(), "0");
					if(!NumberUtils.isNumber(v1)) {
						v1 = "0";
					}
					if(!NumberUtils.isNumber(v2)) {
						v2 = "0";
					}
					myAcc.add(1L);
					if(Long.valueOf(v1) >= Long.valueOf(v2)) {
						return t1;
					} else {
						return t2;
					}
				}
			});
			lastTime = convert(maxTime._2, incrementColumn, "");
		}
		
		/**
		 * 向hbase load 数据
		 */
		loadToHbase(flatMapToPair, hbaseTable, tabName);
		
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
	
	private static String convert(
			List<Tuple2<ImmutableBytesWritable, KeyValue>> list,
			String fieldName, String defaultValue) {
		
		Optional<Tuple2<ImmutableBytesWritable, KeyValue>> ff = list
				.stream()
				.filter(f -> fieldName.equals(new String(CellUtil
						.cloneQualifier(f._2)))).findFirst();
		if(ff.isPresent()) {
			return new String(CellUtil.cloneValue(ff.get()._2));
		}
		return defaultValue;
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
	/**
	 * 向hbase load 数据
	 * @param flatMapToPair
	 * @param tableName
	 * @param tabName
	 * @throws Exception
	 */
	private static void loadToHbase(
			JavaPairRDD<ImmutableBytesWritable, KeyValue> flatMapToPair,
			String tableName, TableName tabName) throws Exception {
		Configuration configuration = HbaseConn.configuration;
		configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		configuration.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, HDFS_TMP_DIR);
		configuration.setInt(
				LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 3200);
		configuration.setLong(
				"hbase.hregion.max.filesize", 10737418240L);
		
		Connection conn = HbaseConn.createConn();
		Table table = conn.getTable(tabName);
		
		Job job = Job.getInstance(configuration);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		
		HRegionLocator regionLocator = new HRegionLocator(tabName,
				(ClusterConnection) conn);
		
		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
		
		String hdfsOutputDir = HDFS_OUTPUT_DIR + System.currentTimeMillis();
		
		flatMapToPair.saveAsNewAPIHadoopFile(hdfsOutputDir,
				ImmutableBytesWritable.class, KeyValue.class,
				HFileOutputFormat2.class, configuration);
		
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
		
		loader.doBulkLoad(new Path(hdfsOutputDir), conn.getAdmin(), table,
				regionLocator);
	}

}

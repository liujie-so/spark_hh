package com.ky.hh;

import static com.ky.util.AccommUtil.PROP_CONFIG;
import static com.ky.util.StringUtils.isBlank;
import static com.ky.util.StringUtils.nvl;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.endsWithIgnoreCase;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.substringBefore;
import static org.apache.commons.lang.StringUtils.trim;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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

import scala.Tuple2;

import com.ky.db.HbaseConn;
import com.ky.util.DateUtils;
import com.ky.util.StringUtils;

/**
 * 补充数据中的trsdr_sjly信息
 * @author liu.jie
 *
 */
public class TestSparkHH5 {
	
	private static final Log log = LogFactory.getLog(TestSparkHH5.class);
	
	private static String HTABLE_COLUMN_FAMILY = "cf1";
	private static byte[] CF_BYTES = toBytes(HTABLE_COLUMN_FAMILY);
	
	private static String oraleTable;
	private static String hbaseTable;
	private static String columns;
	private static String rowkey;
	
	/**
	 * task数量
	 */
	private static final Integer TASK_NUM = Integer.valueOf(PROP_CONFIG.getString("spark.task.num"));
	
	public static void main(String[] args) throws Exception {
		
		if (!checkParam(args)) {
			return;
		}
		
		log.info("------初始化sparkcontext------");
		Class<?>[] classes = { ImmutableBytesWritable.class, KeyValue.class,
				Put.class, ImmutableBytesWritable.Comparator.class };
		SparkConf conf = new SparkConf()
				.setAppName("copyData_" + oraleTable)
				.setMaster("yarn")
				.set("spark.serializer",
						"org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(classes);
		
		SparkSession spark = SparkSession.builder().config(conf)
				.enableHiveSupport().getOrCreate();
		
		
		Dataset<Row> ds = spark.read().format("jdbc").option("url", "jdbc:oracle:thin:@86.1.41.69:1521:orcl")
				.option("user", "ynfk_ztk").option("password", "ynfkztk6089").option("dbtable", oraleTable)
				.option("fetchsize", "100000").load();
		
		ds.createOrReplaceTempView("data_table");
		
		log.info("执行 sql: " + oraleTable + "\t");
		
		Dataset<Row> dataset = spark.sql("select " + columns + " from data_table");
		dataset.show();
		
//		log.info("总量 count: " + dataset.count() + "\t");
		log.info("输出列：" + Arrays.toString(dataset.columns()) + "\t");
		
		TableName tabName = TableName.valueOf(hbaseTable);
		HbaseConn.createTable(tabName, HTABLE_COLUMN_FAMILY);
		
		if(StringUtils.isAllUpperCase(dataset.columns()[0])) {
			rowkey = rowkey.toUpperCase();
		}
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//		Broadcast<String> incColumn = jsc.broadcast(incrementColumn);
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
										toBytes(f.toLowerCase()),
										toBytes(convert(row, f))));
						return new Tuple2<>(rowkey, new Tuple2<>(writable, put));
					}
				}).reduceByKey((v1, v2) -> v2, TASK_NUM).cache();
		
		JavaPairRDD<ImmutableBytesWritable, Put> flatMapToPair;
		flatMapToPair = mapToPair.mapToPair(t -> t._2).sortByKey();
		
		Configuration hdpConf = HBaseConfiguration.create(HbaseConn.configuration);
		hdpConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable);
		
		Job job = Job.getInstance(hdpConf);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		flatMapToPair.saveAsNewAPIHadoopDataset(job.getConfiguration());
		
//		log.info("=======================打印 count: " + flatMapToPair.count());
		
		spark.stop();
		spark.close();
	}
	
	private static String convert(Row row, String fieldName) {
		return convert(row, fieldName, "");
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
			}
			return nvl(trim(value.toString()), defaultValue);
		}
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
	
	private static boolean checkParam(String[] args) throws ParseException {
		Options options = new Options();
		options.addOption("oracletb", true, "oracle表名");
		options.addOption("hbasetb", true, "hbase表名");
		options.addOption("columns", true, "oracle输出列名");
		options.addOption("rowkey", true, "hbase rowkey名");
		PosixParser parser = new PosixParser();
		
		CommandLine cmd = parser.parse(options, args);
		
		if (cmd.hasOption("oracletb")) {
			oraleTable = cmd.getOptionValue("oracletb");
		} else {
			log.info("oracle表名，不能为空！");
			return false;
		}
		if (cmd.hasOption("hbasetb")) {
			hbaseTable = cmd.getOptionValue("hbasetb");
		} else {
			log.info("hbase表名，不能为空！");
			return false;
		}
		if (cmd.hasOption("columns")) {
			columns = cmd.getOptionValue("columns").replaceAll("\"", "");
		} else {
			log.info("columns列名，不能为空！");
			return false;
		}
		if (cmd.hasOption("rowkey")) {
			rowkey = cmd.getOptionValue("rowkey");
		} else {
			log.info("rowkey名，不能为空！");
			return false;
		}
		
		return true;
	}

}

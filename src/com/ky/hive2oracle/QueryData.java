package com.ky.hive2oracle;

import static com.ky.util.AccommUtil.PROP_CONFIG;
import static com.ky.util.DateUtils.ABBR_DATE_FORMATER;
import static com.ky.util.DateUtils.STANDARD_DATE_FORMATER;
import static com.ky.util.DateUtils.formatDate;
import static com.ky.util.DateUtils.parseFullDate;

import java.sql.Types;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MetadataBuilder;

import scala.Option;

import com.google.gson.GsonBuilder;
import com.ky.db.KxOracleConn;
import com.ky.db.OracleConn;
import com.ky.util.StringUtils;

public class QueryData {
	
	private static final Integer BATCH_SIZE = Integer.valueOf(PROP_CONFIG.getString("oracle.batch.size"));
	
	static {
		JdbcDialects.unregisterDialect(JdbcDialects.get(KxOracleConn.KX_URL));
		JdbcDialects.registerDialect(new MyOracleDialect());
	}

	public static void main(String[] args) {
		Logger.getGlobal().info("------args: " + args[0]);
		Config cfg = new GsonBuilder().create().fromJson(args[0], Config.class);
		Logger.getGlobal().info("------初始化sparkcontext------");
		Class<?>[] classes = { ImmutableBytesWritable.class, KeyValue.class,
				Put.class, ImmutableBytesWritable.Comparator.class };
		SparkConf conf = new SparkConf().setAppName("QueryData_" + cfg.getHive_tbname()).setMaster("yarn")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(classes);
		SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		
		String sql = "select " + cfg.getHive_columns() + " from " + cfg.getHive_tbname();
		if(StringUtils.isNotBlank(cfg.getHive_columns_cjsj_lasttime())) {
			if("timestamp".equals(cfg.getHive_columns_cjsj_type())) {
				sql += " where "
						+ cfg.getHive_columns_cjsj()
						+ " > "
						+ "'"
						+ formatDate(parseFullDate(cfg.getHive_columns_cjsj_lasttime(), ABBR_DATE_FORMATER),
								STANDARD_DATE_FORMATER) + "'";
			} else {
				sql += " where " + cfg.getHive_columns_cjsj() + " > '" + cfg.getHive_columns_cjsj_lasttime()
						+ "'";
			}
		}
		
		Logger.getGlobal().info("------hive sql: " + sql);
		Dataset<Row> dataset = spark.sql(sql);
		Logger.getGlobal().info("------dataset.count: " + dataset.count());
		
		Properties prop = new Properties();
		prop.setProperty("user", KxOracleConn.KX_USER);
		prop.setProperty("password", KxOracleConn.KX_PASSWORD);
		dataset.write().mode(SaveMode.Append).option("batchsize", BATCH_SIZE).option("numPartitions", 5)
				.option("driver", OracleConn.driver).jdbc(KxOracleConn.KX_URL, cfg.getOracle_tbname(), prop);
		
	}
	
	class Config{
		private String hive_tbname;
		private String hive_columns;
		private String hive_columns_cjsj;
		private String hive_columns_cjsj_lasttime;
		private String hive_columns_cjsj_type;
		private String oracle_tbname;
		private String trsdr_sjly;
		public String getHive_tbname() {
			return hive_tbname;
		}
		public void setHive_tbname(String hive_tbname) {
			this.hive_tbname = hive_tbname;
		}
		public String getHive_columns() {
			return hive_columns;
		}
		public void setHive_columns(String hive_columns) {
			this.hive_columns = hive_columns;
		}
		public String getHive_columns_cjsj() {
			return hive_columns_cjsj;
		}
		public void setHive_columns_cjsj(String hive_columns_cjsj) {
			this.hive_columns_cjsj = hive_columns_cjsj;
		}
		public String getHive_columns_cjsj_lasttime() {
			return hive_columns_cjsj_lasttime;
		}
		public void setHive_columns_cjsj_lasttime(String hive_columns_cjsj_lasttime) {
			this.hive_columns_cjsj_lasttime = hive_columns_cjsj_lasttime;
		}
		public String getHive_columns_cjsj_type() {
			return hive_columns_cjsj_type;
		}
		public void setHive_columns_cjsj_type(String hive_columns_cjsj_type) {
			this.hive_columns_cjsj_type = hive_columns_cjsj_type;
		}
		public String getOracle_tbname() {
			return oracle_tbname;
		}
		public void setOracle_tbname(String oracle_tbname) {
			this.oracle_tbname = oracle_tbname;
		}
		public String getTrsdr_sjly() {
			return trsdr_sjly;
		}
		public void setTrsdr_sjly(String trsdr_sjly) {
			this.trsdr_sjly = trsdr_sjly;
		}
	}
}

class MyOracleDialect extends JdbcDialect {
	/**
		 * 
		 */
	private static final long serialVersionUID = -7640649652813970855L;

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:oracle");
	}

	@Override
	public String quoteIdentifier(String colName) {
		return colName;
	}

	@Override
	public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
		if (sqlType == Types.NUMERIC) {
			long scale = null != md ? md.build().getLong("scale") : 0L;
			Option<DataType> option = Option.empty();
			if (size == 0) {
				option = Option.apply(DecimalType.apply(DecimalType.MAX_PRECISION(), 10));
			} else if (size == 1) {
				option = Option.apply(new BooleanType());
			} else if (size == 3 || size == 5 || size == 10) {
				option = Option.apply(new IntegerType());
			} else if (size == 19) {
				if (scale == 0L) {
					option = Option.apply(new LongType());
				} else if (scale == 4L) {
					option = Option.apply(new FloatType());
				}
			} else {
				if (scale == -127L) {
					option = Option.apply(DecimalType.apply(DecimalType.MAX_PRECISION(), 10));
				}
			}

			return option;
		} else {
			return Option.empty();
		}
	}

	@Override
	public Option<JdbcType> getJDBCType(DataType dt) {
		Option<JdbcType> option = Option.empty();
		if (DataTypes.BooleanType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("NUMBER(1)", java.sql.Types.BOOLEAN));
		} else if (DataTypes.IntegerType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("NUMBER(10)", java.sql.Types.INTEGER));
		} else if (DataTypes.LongType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("NUMBER(19)", java.sql.Types.BIGINT));
		} else if (DataTypes.FloatType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("NUMBER(19, 4)", java.sql.Types.FLOAT));
		} else if (DataTypes.DoubleType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("NUMBER(19, 4)", java.sql.Types.DOUBLE));
		} else if (DataTypes.ByteType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("NUMBER(3)", java.sql.Types.SMALLINT));
		} else if (DataTypes.ShortType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("NUMBER(5)", java.sql.Types.SMALLINT));
		} else if (DataTypes.StringType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("VARCHAR2(255)", java.sql.Types.VARCHAR));
		} else if(DataTypes.BinaryType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("BINARY", java.sql.Types.BINARY));
		} else if(DataTypes.TimestampType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("DATE", java.sql.Types.TIMESTAMP));
		} else if(DataTypes.DateType.sameType(dt)) {
			option = Option.apply(JdbcType.apply("DATE", java.sql.Types.DATE));
		}
		return option;
	}
}
package com.ky.db;

import static com.ky.util.AccommUtil.PROP_CONFIG;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseConn {
	
	private static final String address = PROP_CONFIG.getString("zookeeper.address");
	private static final String parent = PROP_CONFIG.getString("hbase.zookeeper.parent");
	private static final String id = PROP_CONFIG.getString("hbase.id");
	private static final String key = PROP_CONFIG.getString("hbase.key");
	public static Configuration configuration;
	
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", address);
		configuration.set("zookeeper.znode.parent", parent);
		configuration.set("hbase.security.authentication.tbds.secureid", id);
		configuration.set("hbase.security.authentication.tbds.securekey", key);
	}
	
	private static Connection conn = null;
	
	public static Connection createConn() throws IOException {
		if (conn == null) {
			conn = ConnectionFactory.createConnection(configuration);
		}
		return conn;
	}
	
	public static boolean createTable(TableName tableName,
			String... columnFamilys) throws Exception {
		
		if(ArrayUtils.isEmpty(columnFamilys)) {
			return false;
		}
		
		Connection cccon = createConn();
		Admin admin = cccon.getAdmin();
		if (!admin.tableExists(tableName)) {
			System.out.println("start create table:"+tableName);
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			Arrays.asList(columnFamilys).stream()
					.forEach(f -> tableDescriptor.addFamily(new HColumnDescriptor(f)));
			admin.createTable(tableDescriptor);
			System.out.println("end create table:"+tableName);
		}
		admin.close();
		
		return true;
		
	}
}

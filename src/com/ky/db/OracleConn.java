package com.ky.db;

import static com.ky.util.AccommUtil.PROP_CONFIG;
import static com.ky.util.StringUtils.isBlank;
import static com.ky.util.MapUtils.create;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ky.util.MapUtils;
import com.ky.util.StringUtils;

public class OracleConn {
	
	private static final Log log = LogFactory.getLog(OracleConn.class);
	
	public static final String driver = PROP_CONFIG.getString("oracle.driver");
	private static final String url = PROP_CONFIG.getString("oracle.url");
	public static final String user = PROP_CONFIG.getString("oracle.user");
	private static final String password = PROP_CONFIG.getString("oracle.password");
	private static final String ORDER_SUFFIX = "_order";
	
	static {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static Connection getOracleConn() throws SQLException {
		return DriverManager.getConnection(url, user, password);
	}
	
	/**
	 * 修改数据
	 * @param sql
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public static int updateSql(String sql, Object... args) {
		if(StringUtils.isBlank(sql)) {
			return 0;
		}
		int count = 0;
		try (Connection c = getOracleConn(); PreparedStatement pstm = createPs(c, sql, args);) {
			System.out.println("updateSql: " + sql + "，参数: " + Arrays.toString(args));
			count = pstm.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage(), e);
		}
		return count;
	}
	
	public static int execute(String sql, Object[]... args) {
		if (isBlank(sql) || ArrayUtils.isEmpty(args)) {
			return 0;
		}
		System.out.println("============================execute sql: " + sql);
		try (Connection c = getOracleConn(); PreparedStatement pstm = c.prepareStatement(sql)) {
			for (Object[] arg : args) {
				for (int i = 0; i < arg.length; i++) {
					if (arg[i] instanceof String) {
						pstm.setString(i + 1, StringUtils.nvl(arg[i]));
					} else if (arg[i] instanceof Date) {
						pstm.setTimestamp(i + 1, new Timestamp(((Date) arg[i]).getTime()));
					} else {
						pstm.setInt(i + 1, Integer.parseInt(StringUtils.nvl(args[i], "0")));
					}
				}
				pstm.addBatch();
			}
			int[] ints = pstm.executeBatch();
			return Arrays.stream(ints).sum();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	public static int updateSql(String sql) {
		return updateSql(sql, ArrayUtils.EMPTY_OBJECT_ARRAY);
	}
	/**
	 * 插入数据
	 * @param tableName
	 * @param map
	 * @return
	 * @throws Exception
	 */
	public static int insertSql(String tableName, Map<String, Object> map) {
		return insertSql(tableName, Arrays.asList(map));
	}
	
	public static int insertSql(String tableName, List<Map<String, ?>> rows) {
		if (CollectionUtils.isEmpty(rows)) {
			return 0;
		}
		final Set<String> ent = rows.get(0).keySet();
		String columns = ent.stream().collect(Collectors.joining(","));
		String values = ent.stream().map(f -> "?").collect(Collectors.joining(","));
		
		List<Object[]> collect = rows.stream()
				.map(f -> ent.stream().map(k -> f.get(k)).collect(Collectors.toList()).toArray())
				.collect(Collectors.toList());

		return execute("insert into " + tableName + "(" + columns + ") values(" + values + ")",
				collect.toArray(new Object[][] {}));
	}
	
	public static int insertSql(String tableName, Object... kvs) {
		if (kvs == null || kvs.length % 2 != 0) {
			return 0;
		}
		return insertSql(tableName, create(kvs));
	}
	
	public static List<Map<String, String>> list(String tableName, Map<String, Object> whereRegular,
			String... columns) {
		StringBuilder sql = new StringBuilder("select ");
		String outputs = "*";
		if(ArrayUtils.isNotEmpty(columns)) {
			outputs = Arrays.asList(columns).stream().collect(Collectors.joining(","));
		}
		sql.append(outputs).append(" from ").append(tableName).append(" where 1=1 ");
		/**
		 * 参数列表
		 */
		List<Object> params = Lists.newArrayList();
		if(MapUtils.isNotEmpty(whereRegular)) {
			String orderPart = "";
			for (Map.Entry<String, Object> regular : whereRegular.entrySet()) {
				if(isBlank(regular.getValue())) {
					continue;
				}
				if(StringUtils.endsWithIgnoreCase(regular.getKey(), ORDER_SUFFIX)) {
					orderPart = " order by " + StringUtils.removeEnd(regular.getKey(), ORDER_SUFFIX) + " "
							+ regular.getValue();
					continue;
				}
				sql.append(" and ").append(regular.getKey()).append("?");
				params.add(regular.getValue());
			}
			sql.append(orderPart);
		}
		return list(sql.toString(), params.toArray());
	}
	
	public static List<Map<String, String>> list(String sql, Object[] args) {

		System.out.println("=========打印 sql: " + sql + "，参数: " + Arrays.toString(args));

		List<String> outputColumns = Lists.newArrayList();
		List<Map<String, String>> res = Lists.newArrayList();
		try (Connection innerConn = getOracleConn();
				PreparedStatement pstm = createPs(innerConn, sql, args);
				ResultSet rs = pstm.executeQuery();) {
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				outputColumns.add(metaData.getColumnName(i).toLowerCase());
			}
			while (rs.next()) {
				Map<String, String> data = Maps.newHashMap();
				for (int i = 0; i < outputColumns.size(); i++) {
					data.put(outputColumns.get(i), rs.getString(outputColumns.get(i)));
				}
				res.add(data);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return res;
	}
	
	public static PreparedStatement createPs(Connection c, String sql, Object[] args) throws SQLException {
		PreparedStatement ps = c.prepareStatement(sql);
		if(ArrayUtils.isNotEmpty(args)) {
			for (int i = 0; i < args.length; i++) {
				if(args[i] instanceof String) {
					ps.setString(i + 1, StringUtils.nvl(args[i]));
				} else if(args[i] instanceof Date) {
					ps.setTimestamp(i + 1, new Timestamp(((Date) args[i]).getTime()));
				} else {
					ps.setInt(i + 1, Integer.parseInt(StringUtils.nvl(args[i], "0")));
				}
			}
		}
		return ps;
	}
}

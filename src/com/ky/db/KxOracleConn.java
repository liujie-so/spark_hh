package com.ky.db;

import static com.ky.util.AccommUtil.PROP_CONFIG;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class KxOracleConn extends OracleConn {

	public static final String KX_URL = PROP_CONFIG.getString("write.to.oracle.url");
	public static final String KX_USER = PROP_CONFIG.getString("write.to.oracle.username");
	public static final String KX_PASSWORD = PROP_CONFIG.getString("write.to.oracle.password");
	
	public static Connection getOracleConn() throws SQLException {
		return DriverManager.getConnection(KX_URL, KX_USER, KX_PASSWORD);
	}
}

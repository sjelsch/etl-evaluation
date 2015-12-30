package org.rdfqb2kylin.connections;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.etl.Property;

/*
 * Hive JDBC Connection
 * Handles HiveQL Requests to Hive Server
 */
public class HiveConnection {
	private final static Logger logger = Logger.getLogger(HiveConnection.class);

	private static String ConnectionURL = "jdbc:hive2://" + Property.get("hive.url");

	private static HiveConnection instance;
	private Connection connection;

	public HiveConnection() {
		try {
			Class.forName(Property.get("hive.jdbc"));
			connection = DriverManager.getConnection(ConnectionURL, Property.get("hive.user"), Property.get("hive.password"));
			logger.info("Hive Connection to " + Property.get("hive.url") + " Established...\n");
		} catch (Exception e) {
			logger.error("Hive Connection Failed: " + e);
		}
	}

	public static synchronized Connection getInstance() {
		if (instance == null) {
			instance = new HiveConnection();
		}
		return instance.connection;
	}
}

package org.rdfqb2kylin.connections;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.etl.Property;

/*
 * Kylin JDBC Connection
 * Handles SQL Requests to Kylin
 */
public class KylinConnection {
	private final static Logger logger = Logger.getLogger(KylinConnection.class);

	private static KylinConnection instance;
	private Connection connection;

	public KylinConnection(String kylinProjectName) {
		try {
			Class.forName(Property.get("kylin.jdbc"));
			connection = DriverManager.getConnection("jdbc:kylin://" + Property.get("kylin.server") + ":7070/" + kylinProjectName, Property.get("kylin.user"), Property.get("kylin.password"));
			logger.info("Kylin Connection to " + Property.get("kylin.url") + " Established...\n");
		} catch (Exception e) {
			logger.error("Kylin Connection Failed: " + e);
		}
	}

	public static synchronized Connection getInstance(String kylinProjectName) {
		if (instance == null) {
			instance = new KylinConnection(kylinProjectName);
		}
		return instance.connection;
	}
}

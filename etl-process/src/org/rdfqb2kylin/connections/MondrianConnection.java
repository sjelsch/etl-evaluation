package org.rdfqb2kylin.connections;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.etl.Property;

/*
 * Mondrian JDBC Connection
 * Handles MDX Requests to Kylin via Mondrian
 */
public class MondrianConnection {
	private final static Logger logger = Logger.getLogger(MondrianConnection.class);

	private static MondrianConnection instance;
	private Connection connection;

	public MondrianConnection(String mondrianPath, String kylinProjectName) {
		try {
			Class.forName(Property.get("mondrian.jdbc"));
			connection = DriverManager.getConnection(
		        "jdbc:mondrian:"
		      + " Jdbc=jdbc:kylin://" + Property.get("kylin.server") + ":7070/" + kylinProjectName + ";"
		      + " JdbcDrivers="+Property.get("kylin.jdbc")+";"
		      + " JdbcUser="+Property.get("kylin.user")+";"
		      + " JdbcPassword="+Property.get("kylin.password")+";"
		      + " Catalog=file:"+mondrianPath+";");

			logger.info("Mondrian Connection to " + Property.get("kylin.url") + " Established...\n");
		} catch (Exception e) {
			logger.error("Mondrian Connection Failed: " + e);
		}
	}

	public static synchronized Connection getInstance(String mondrianPath, String kylinProjectName) {
		if (instance == null) {
			instance = new MondrianConnection(mondrianPath, kylinProjectName);
		}
		return instance.connection;
	}
}

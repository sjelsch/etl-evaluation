package org.rdfqb2kylin.etl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

/*
 * Property Class
 * Reads config properties from config.properties File
 */
public class Property {
	private final static String configFile = "config.properties";
	private static HashMap<String, String> properties = new HashMap<String, String>();

	static {
		Properties properties = new Properties();

		try {
			File file = new File("./" + configFile);

			// If Property File is available at root directory, use that config file.
			if (file.exists() && !file.isDirectory()) {
				FileInputStream fileIS = new FileInputStream("./" + configFile);
				properties.load(fileIS);
				fileIS.close();

			// No config properties file found on root directory, use file in ressources directory
			} else {
				InputStream inputStream = Property.class.getClassLoader().getResourceAsStream(configFile);
				properties.load(inputStream);
				inputStream.close();
			}

			for (Object key : (Set<Object>) properties.keySet()) {
				Property.add(key.toString(), properties.get(key).toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void add(String key, String value) {
		properties.put(key.toString(), value);
	}

	public static String get(String key) {
		return properties.get(key);
	}

	public static boolean useParquet() {
		if (Property.get("hive.use_parquet").equals("true")) {
			return true;
		}

		return false;
	}

	public static boolean usePartition() {
		if (Property.get("hive.use_partition").equals("true")) {
			return true;
		}

		return false;
	}

	public static boolean useCompression() {
		if (useParquet() || usePartition()) {
			return true;
		}

		return false;
	}

	// If optimization is active, use 'hive.optimized_triples_table', else use 'hive.triples_table' as Triples table
	public static String getQBTriplesName() {
		if (Property.useCompression()) {
			return Property.get("hive.optimized_triples_table");
		}

		return Property.get("hive.triples_table");
	}
}

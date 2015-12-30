package org.rdfqb2kylin.util;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.etl.Property;

public class UriUtil {
	private static String[] AGGREGATORS = { "sum", "avg", "min", "max", "count" };

	private final static Logger logger = Logger.getLogger(UriUtil.class);

	public static boolean isUri(String string) {
		if (string != null && string.startsWith("<") && string.endsWith(">")) {
			return true;
		} else {
			return false;
		}
	}

	public static String clean(String string) {
		if (isUri(string)) {
			return string.replace("<", "").replace(">", "").split("#")[1];
		}
		return string;
	}

	/*
	 * Possible aggregate functions from http://purl.org/qb4olap/cubes:
	 * http://purl.org/qb4olap#avg, http://purl.org/qb4olap#count,
	 * http://purl.org/qb4olap#max, http://purl.org/qb4olap#min,
	 * http://purl.org/qb4olap#sum
	 */
	public static String convertToAggregator(String uri) {
		if (isUri(uri)) {
			String aggregator = uri.replace(Property.get("rdf.prefix.qb4o") + "#", "").replace("<", "").replace(">", "");

			if (!Arrays.asList(AGGREGATORS).contains(aggregator)) {
				logger.error("Could not find any aggregation function for '" + aggregator + "'. Return standard aggregator: sum");
				return "sum";
			}

			return aggregator;
		}
		return uri;
	}

	public static String getStandardAggregator() {
		return AGGREGATORS[0];
	}

	public static String build(String prefix, String name) {
		return "<" + Property.get("rdf.prefix." + prefix) + "#" + name + ">";
	}
}

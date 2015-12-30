package org.rdfqb2kylin.util;

public class StringUtil {
	public static String removeQuotes(String s) {
		return s.replace("\"", "");
	}

	public static int toInt(String s) {
		return Integer.parseInt(removeQuotes(s));
	}
}

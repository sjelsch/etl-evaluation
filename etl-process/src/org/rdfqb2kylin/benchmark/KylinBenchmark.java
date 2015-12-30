package org.rdfqb2kylin.benchmark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.connections.KylinConnection;
import org.rdfqb2kylin.etl.Property;
import org.rdfqb2kylin.util.DBTablePrinter;

/*
 * SQL Benchmark Class
 * Includes 13 SQL OLAP queries for Kylin
 */
public class KylinBenchmark {
	private String kylinProjectName;

	private final static Logger logger = Logger.getLogger(KylinBenchmark.class);

	public KylinBenchmark(String kylinProjectName) {
		this.kylinProjectName = kylinProjectName;
	}

	public void run() throws ClassNotFoundException, SQLException, FileNotFoundException {
		Connection connection = KylinConnection.getInstance(kylinProjectName);

		long startTS;
		long endTS;

		HashMap<Integer, String> mdxList = new HashMap<Integer, String>();
		mdxList.put(1, query1());
		mdxList.put(2, query2());
		mdxList.put(3, query3());
		mdxList.put(4, query4());
		mdxList.put(5, query5());
		mdxList.put(6, query6());
		mdxList.put(7, query7());
		mdxList.put(8, query8());
		mdxList.put(9, query9());
		mdxList.put(10, query10());
		mdxList.put(11, query11());
		mdxList.put(12, query12());
		mdxList.put(13, query13());

		try (Statement statement = connection.createStatement()) {
			for(int i = 1; i <= Integer.parseInt(Property.get("benchmark.runSQLQueriesIterations")); i++) {
				logger.info("");
				logger.info("Iteration: "+i);

				for(Entry<Integer, String> entry : mdxList.entrySet()) {
					startTS = System.currentTimeMillis();
					ResultSet resultSet = statement.executeQuery(entry.getValue());
					endTS = System.currentTimeMillis();
					logger.info("Execution Time SQL Query " + entry.getKey() + ": " + (endTS - startTS));
					writeLogFile(resultSet, entry.getKey(), i, (endTS - startTS));
				}
			}
		} catch (SQLException e) {
			logger.error("Could not execute SQL Query!");
			e.printStackTrace();
			System.exit(0);
		}
	}

	private String query1() {
		return " SELECT "
			+ "   SUM(SUM_REVENUE) AS REVENUE "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " WHERE "
			+ "   LO_ORDERDATEYEARLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1993>' "
			+ "   AND LO_DISCOUNT BETWEEN 1.0 AND 3.0 "
			+ "   AND LO_QUANTITY < 25 ";
	}

	private String query2() {
		return " SELECT "
			+ "   SUM(SUM_REVENUE) AS REVENUE "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " WHERE "
			+ "   LO_ORDERDATEYEARMONTHNUMLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYearMonthNum199401>' "
			+ "   AND LO_DISCOUNT BETWEEN 4.0 AND 6.0 "
			+ "   AND LO_QUANTITY BETWEEN 26.0 AND 35.0 ";
	}

	private String query3() {
		return " SELECT "
			+ "   SUM(SUM_REVENUE) AS REVENUE "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " WHERE "
			+ "   LO_ORDERDATEWEEKNUMINYEARLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateWeeknuminyear19946>' "
			+ "   AND LO_ORDERDATEYEARLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1994>' "
			+ "   AND LO_DISCOUNT BETWEEN 5.0 AND 7.0 "
			+ "   AND LO_QUANTITY BETWEEN 26.0 AND 35.0 ";
	}

	private String query4() {
		return " SELECT "
			+ "     sum(LO_REVENUE) as LO_REVENUE, LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL "
			+ " FROM "
			+ "     FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_PARTKEY ON (FACTS.LO_PARTKEY = LO_PARTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " WHERE "
			+ "     LO_PARTKEYCATEGORYLEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyCategoryMFGR-12>' "
			+ "     and LO_SUPPKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionAMERICA>' "
			+ " GROUP BY "
			+ "     LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL "
			+ " ORDER BY "
			+ "     LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL";
	}

	private String query5() {
		return " SELECT "
			+ "   SUM(LO_REVENUE) AS LO_REVENUE, LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_PARTKEY ON (FACTS.LO_PARTKEY = LO_PARTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " WHERE "
			+ "   ( "
			+ "     LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2221>'  "
			+ "     OR  LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2222>' "
			+ "     OR  LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2223>' "
			+ "     OR  LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2224>' "
			+ "     OR  LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2225>' "
			+ "     OR  LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2226>' "
			+ "     OR  LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2227>' "
			+ "     OR  LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2228>' "
			+ "   ) "
			+ "   AND LO_SUPPKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionASIA>' "
			+ " GROUP BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL";
	}

	private String query6() {
		return " SELECT "
			+ "   SUM(LO_REVENUE) AS LO_REVENUE, LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_PARTKEY ON (FACTS.LO_PARTKEY = LO_PARTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " WHERE "
			+ "   LO_PARTKEYBRAND1LEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2239>' "
			+ "   AND LO_SUPPKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionEUROPE>' "
			+ " GROUP BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_PARTKEYBRAND1LEVEL ";
	}

	private String query7() {
		return " SELECT "
			+ "   LO_CUSTKEYNATIONLEVEL, LO_SUPPKEYNATIONLEVEL, LO_ORDERDATEYEARLEVEL, "
			+ "   SUM(LO_REVENUE) AS LO_REVENUE "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_CUSTKEY ON (FACTS.LO_CUSTKEY = LO_CUSTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " WHERE "
			+ "   LO_CUSTKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_custkeyRegionASIA>' "
			+ "   AND LO_SUPPKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionASIA>' "
			+ "   AND LO_ORDERDATEYEARLEVEL >= '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1992>' "
			+ "   AND LO_ORDERDATEYEARLEVEL <= '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>' "
			+ " GROUP BY "
			+ "   LO_CUSTKEYNATIONLEVEL, LO_SUPPKEYNATIONLEVEL, LO_ORDERDATEYEARLEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL ASC, LO_REVENUE DESC ";
	}

	private String query8() {
		return " SELECT "
			+ "   LO_CUSTKEYCITYLEVEL, LO_SUPPKEYCITYLEVEL, LO_ORDERDATEYEARLEVEL, SUM(LO_REVENUE) AS LO_REVENUE "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_CUSTKEY ON (FACTS.LO_CUSTKEY = LO_CUSTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " WHERE "
			+ "   LO_CUSTKEYNATIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_custkeyNationUNITED-STATES>' "
			+ "   AND LO_SUPPKEYNATIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_suppkeyNationUNITED-STATES>' "
			+ "   AND LO_ORDERDATEYEARLEVEL >= '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1992>' "
			+ "   AND LO_ORDERDATEYEARLEVEL <= '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>' "
			+ " GROUP BY "
			+ "   LO_CUSTKEYCITYLEVEL, LO_SUPPKEYCITYLEVEL, LO_ORDERDATEYEARLEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL ASC, LO_REVENUE DESC ";
	}

	private String query9() {
		return " SELECT "
			+ "   LO_CUSTKEYCITYLEVEL, LO_SUPPKEYCITYLEVEL, LO_ORDERDATEYEARLEVEL, SUM(LO_REVENUE) AS LO_REVENUE "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_CUSTKEY ON (FACTS.LO_CUSTKEY = LO_CUSTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " WHERE "
			+ "   (LO_CUSTKEYCITYLEVEL='<http://lod2.eu/schemas/rdfh#lo_custkeyCityUNITED-KI1>' OR LO_CUSTKEYCITYLEVEL='<http://lod2.eu/schemas/rdfh#lo_custkeyCityUNITED-KI5>') "
			+ "   AND (LO_SUPPKEYCITYLEVEL='<http://lod2.eu/schemas/rdfh#lo_suppkeyCityUNITED-KI1>' OR LO_SUPPKEYCITYLEVEL='<http://lod2.eu/schemas/rdfh#lo_suppkeyCityUNITED-KI5>') "
			+ "   AND LO_ORDERDATEYEARLEVEL >= '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1992>' "
			+ "   AND LO_ORDERDATEYEARLEVEL <= '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>' "
			+ " GROUP BY "
			+ "   LO_CUSTKEYCITYLEVEL, LO_SUPPKEYCITYLEVEL, LO_ORDERDATEYEARLEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL ASC, LO_REVENUE DESC ";
	}

	private String query10() {
		return " SELECT "
			+ "   LO_CUSTKEYCITYLEVEL, LO_SUPPKEYCITYLEVEL, LO_ORDERDATEYEARLEVEL, SUM(LO_REVENUE) AS LO_REVENUE "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_CUSTKEY ON (FACTS.LO_CUSTKEY = LO_CUSTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " WHERE "
			+ "   (LO_CUSTKEYCITYLEVEL='<http://lod2.eu/schemas/rdfh#lo_custkeyCityUNITED-KI1>' OR LO_CUSTKEYCITYLEVEL='<http://lod2.eu/schemas/rdfh#lo_custkeyCityUNITED-KI5>') "
			+ "   AND (LO_SUPPKEYCITYLEVEL='<http://lod2.eu/schemas/rdfh#lo_suppkeyCityUNITED-KI1>' OR LO_SUPPKEYCITYLEVEL='<http://lod2.eu/schemas/rdfh#lo_suppkeyCityUNITED-KI5>') "
			+ "   AND LO_ORDERDATEYEARMONTHLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYearMonthDec1997>' "
			+ " GROUP BY "
			+ "   LO_CUSTKEYCITYLEVEL, LO_SUPPKEYCITYLEVEL, LO_ORDERDATEYEARLEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL ASC, LO_REVENUE DESC ";
	}

	private String query11() {
		return " SELECT "
			+ "   LO_ORDERDATEYEARLEVEL, LO_CUSTKEYNATIONLEVEL, SUM(LO_REVENUE) - sum(LO_SUPPLYCOST) AS PROFIT "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_CUSTKEY ON (FACTS.LO_CUSTKEY = LO_CUSTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " INNER JOIN LO_PARTKEY ON (FACTS.LO_PARTKEY = LO_PARTKEY.KEY) "
			+ " WHERE "
			+ "   LO_CUSTKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_custkeyRegionAMERICA>' "
			+ "   AND LO_SUPPKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionAMERICA>' "
			+ "   AND (LO_PARTKEYMFGRLEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyMfgrMFGR-1>' OR LO_PARTKEYMFGRLEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyMfgrMFGR-2>') "
			+ " GROUP BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_CUSTKEYNATIONLEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_CUSTKEYNATIONLEVEL ";
	}

	private String query12() {
		return " SELECT "
			+ "   LO_ORDERDATEYEARLEVEL, LO_SUPPKEYNATIONLEVEL, LO_PARTKEYCATEGORYLEVEL, "
			+ "   SUM(LO_REVENUE) - sum(LO_SUPPLYCOST) AS PROFIT "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_CUSTKEY ON (FACTS.LO_CUSTKEY = LO_CUSTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " INNER JOIN LO_PARTKEY ON (FACTS.LO_PARTKEY = LO_PARTKEY.KEY) "
			+ " WHERE "
			+ "   LO_CUSTKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_custkeyRegionAMERICA>' "
			+ "   AND LO_SUPPKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionAMERICA>' "
			+ "   AND (LO_ORDERDATEYEARLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>' OR LO_ORDERDATEYEARLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1998>') "
			+ "   AND (LO_PARTKEYMFGRLEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyMfgrMFGR-1>' OR LO_PARTKEYMFGRLEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyMfgrMFGR-2>') "
			+ " GROUP BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_SUPPKEYNATIONLEVEL, LO_PARTKEYCATEGORYLEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_SUPPKEYNATIONLEVEL, LO_PARTKEYCATEGORYLEVEL ";
	}

	private String query13() {
		return " SELECT "
			+ "   LO_ORDERDATEYEARLEVEL, LO_SUPPKEYCITYLEVEL, LO_PARTKEYBRAND1LEVEL, "
			+ "   SUM(LO_REVENUE) - sum(LO_SUPPLYCOST) AS PROFIT "
			+ " FROM FACTS "
			+ " INNER JOIN LO_ORDERDATE ON (FACTS.LO_ORDERDATE = LO_ORDERDATE.KEY) "
			+ " INNER JOIN LO_CUSTKEY ON (FACTS.LO_CUSTKEY = LO_CUSTKEY.KEY) "
			+ " INNER JOIN LO_SUPPKEY ON (FACTS.LO_SUPPKEY = LO_SUPPKEY.KEY) "
			+ " INNER JOIN LO_PARTKEY ON (FACTS.LO_PARTKEY = LO_PARTKEY.KEY) "
			+ " WHERE "
			+ "   LO_CUSTKEYREGIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_custkeyRegionAMERICA>' "
			+ "   AND LO_SUPPKEYNATIONLEVEL = '<http://lod2.eu/schemas/rdfh#lo_suppkeyNationUNITED-STATES>' "
			+ "   AND (LO_ORDERDATEYEARLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>' OR LO_ORDERDATEYEARLEVEL = '<http://lod2.eu/schemas/rdfh#lo_orderdateYear1998>') "
			+ "   AND LO_PARTKEYCATEGORYLEVEL = '<http://lod2.eu/schemas/rdfh#lo_partkeyCategoryMFGR-14>' "
			+ " GROUP BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_SUPPKEYCITYLEVEL, LO_PARTKEYBRAND1LEVEL "
			+ " ORDER BY "
			+ "   LO_ORDERDATEYEARLEVEL, LO_SUPPKEYCITYLEVEL, LO_PARTKEYBRAND1LEVEL ";
	}

	private void writeLogFile(ResultSet resultSet, int queryNumber, int iterationNumber, long executionTime) throws FileNotFoundException {
		PrintWriter writer = new PrintWriter("runSQLQuery"+queryNumber+"."+iterationNumber+".log");

		writer.println(DBTablePrinter.printResultSet(resultSet));
		writer.println("Execution Time: " + executionTime);
		writer.println("");
		writer.flush();
		writer.close();
	}
}

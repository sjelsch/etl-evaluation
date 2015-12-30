package org.rdfqb2kylin.benchmark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.rdfqb2kylin.connections.MondrianConnection;
import org.rdfqb2kylin.etl.Property;

/*
 * MDX Benchmark Class
 * Includes 13 MDX OLAP queries for MDX on top of Kylin
 */
public class MondrianBenchmark {
	private String mondrianPath;
	private String kylinProjectName;

	private final static Logger logger = Logger.getLogger(MondrianBenchmark.class);

	public MondrianBenchmark(String mondrianPath, String kylinProjectName) {
		this.mondrianPath = mondrianPath;
		this.kylinProjectName = kylinProjectName;
	}

	public void run() throws ClassNotFoundException, SQLException, FileNotFoundException {
		Connection connection = MondrianConnection.getInstance(mondrianPath, kylinProjectName);

		// We are dealing with an OLAP connection. we must unwrap it.
		final OlapConnection olapConnection = connection.unwrap(OlapConnection.class);

		// Prepare a statement.
		final OlapStatement olapStatement = olapConnection.createStatement();

		long startTS;
		long endTS;
		CellSet cellSet;

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

		for(int i = 1; i <= Integer.parseInt(Property.get("benchmark.runMDXQueriesIterations")); i++) {
			logger.info("");
			logger.info("Iteration: "+i);

			for(Entry<Integer, String> entry : mdxList.entrySet()) {
				startTS = System.currentTimeMillis();
				cellSet = olapStatement.executeOlapQuery(entry.getValue());
				endTS = System.currentTimeMillis();
				logger.info("Execution Time Query " + entry.getKey() + ": " + (endTS - startTS));
				writeLogFile(cellSet, entry.getKey(), i, (endTS - startTS));
			}
		}
	}

	private String query1() {
		return "SELECT "
		      + "  {[Measures].[SUM_REVENUE]} ON COLUMNS, "
		      + "  {[LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateYear1993>]} ON ROWS "
		      + "FROM [" + kylinProjectName + "] "
		      + "WHERE CrossJoin( "
		      + "  Filter( "
		      + "    [LO_DISCOUNT].[LO_DISCOUNT].members, "
		      + "    Cast([LO_DISCOUNT].currentmember.name as NUMERIC) >= 1 "
		      + "    and Cast([LO_DISCOUNT].currentmember.name as NUMERIC) <= 3 "
		      + "  ), "
		      + "  Filter( "
		      + "    [LO_QUANTITY].[LO_QUANTITY].members, "
		      + "    Cast([LO_QUANTITY].currentmember.name as NUMERIC) < 25)"
		      + "  ) "
		      + "";
	}

	private String query2() {
		return "SELECT {[Measures].[SUM_REVENUE]} ON COLUMNS, "
			+ "{[LO_ORDERDATE].[LO_ORDERDATEYEARMONTHNUMLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateYearMonthNum199401>]} ON ROWS "
			+ "FROM [" + kylinProjectName + "] "
			+ "WHERE CrossJoin( "
			+ "  Filter([LO_DISCOUNT].[LO_DISCOUNT].members, Cast([LO_DISCOUNT].currentmember.name as NUMERIC) >= 4 and Cast([LO_DISCOUNT].currentmember.name as NUMERIC) <= 6), "
			+ "  Filter([LO_QUANTITY].[LO_QUANTITY].members, Cast([LO_QUANTITY].currentmember.name as NUMERIC) >= 26 and Cast([LO_QUANTITY].currentmember.name as NUMERIC) <= 35) "
			+ ") ";
	}

	private String query3() {
		return "SELECT {[Measures].[SUM_REVENUE]} ON COLUMNS, "
			+ "{([LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateYear1994>], [LO_ORDERDATE].[LO_ORDERDATEWEEKNUMINYEARLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateWeeknuminyear19946>])} ON ROWS "
			+ "FROM [" + kylinProjectName + "] "
			+ "WHERE CrossJoin( "
			+ "  Filter([LO_DISCOUNT].[LO_DISCOUNT].members, Cast([LO_DISCOUNT].currentmember.name as NUMERIC) >= 5 and Cast([LO_DISCOUNT].currentmember.name as NUMERIC) <= 7), "
			+ "  Filter([LO_QUANTITY].[LO_QUANTITY].members, Cast([LO_QUANTITY].currentmember.name as NUMERIC) >= 26 and Cast([LO_QUANTITY].currentmember.name as NUMERIC) <= 35) "
			+ ") ";
	}

	private String query4() {
		return "SELECT {[Measures].[LO_REVENUE]} ON COLUMNS, "
			+ "{Crossjoin([LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].members, [LO_PARTKEY].[LO_PARTKEYCATEGORYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyCategoryMFGR-12>].children)} ON ROWS "
			+ "FROM [" + kylinProjectName + "] "
			+ "WHERE"
			+ "  [LO_SUPPKEY].[LO_SUPPKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionAMERICA>] ";
	}

	private String query5() {
		return "SELECT {[Measures].[LO_REVENUE]} ON COLUMNS, "
			+ "{Crossjoin([LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].members, {"
			+ "  [LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2221>],"
			+ "  [LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2222>],"
			+ "  [LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2223>],"
			+ "  [LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2224>],"
			+ "  [LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2225>],"
			+ "  [LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2226>],"
			+ "  [LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2227>],"
			+ "  [LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2228>]}"
			+ ")} ON ROWS "
			+ "FROM [" + kylinProjectName + "] "
			+ "WHERE "
			+ "  [LO_SUPPKEY].[LO_SUPPKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionASIA>]";
	}

	private String query6() {
		return "SELECT {[Measures].[LO_REVENUE]} ON COLUMNS, "
			+ "{Crossjoin([LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].members, {[LO_PARTKEY].[LO_PARTKEYBRAND1LEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyBrand1MFGR-2239>]})} ON ROWS "
			+ "FROM [" + kylinProjectName + "] "
			+ "WHERE [LO_SUPPKEY].[LO_SUPPKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionEUROPE>]";
	}

	private String query7() {
		return " SELECT " +
			"   {[Measures].[LO_REVENUE]} ON COLUMNS, " +
			"   Crossjoin( " +
			"     [LO_CUSTKEY].[LO_CUSTKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyRegionASIA>].children, " +
			"     Crossjoin( " +
			"       [LO_SUPPKEY].[LO_SUPPKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionASIA>].children, " +
			"       FILTER( " +
			"         [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].members, " +
			"         [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1992>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1993>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1994>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1995>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1996>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>\" " +
			"       ) " +
			"     ) " +
			"   ) ON ROWS " +
			" FROM [" + kylinProjectName + "] ";
	}

	private String query8() {
		return "SELECT {[Measures].[LO_REVENUE]} ON COLUMNS, " +
			"Crossjoin([LO_CUSTKEY].[LO_CUSTKEYNATIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyNationUNITED-STATES>].children, Crossjoin([LO_SUPPKEY].[LO_SUPPKEYNATIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyNationUNITED-STATES>].children,  " +
			"FILTER(" +
			"  [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].members," +
			"         [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1992>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1993>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1994>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1995>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1996>\" " +
			"         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>\" " +
			"))) ON ROWS " +
			" FROM [" + kylinProjectName + "] ";
	}

	private String query9() {
		return "SELECT {[Measures].[LO_REVENUE]} ON COLUMNS, "
			+ "Crossjoin({[LO_CUSTKEY].[LO_CUSTKEYCITYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyCityUNITED-KI1>], [LO_CUSTKEY].[LO_CUSTKEYCITYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyCityUNITED-KI5>]}, Crossjoin({[LO_SUPPKEY].[LO_SUPPKEYCITYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyCityUNITED-KI1>], [LO_SUPPKEY].[LO_SUPPKEYCITYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyCityUNITED-KI5>]}, "
			+ "FILTER([LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].members, "
			+ "         [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1992>\" "
			+ "         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1993>\" "
			+ "         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1994>\" "
			+ "         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1995>\" "
			+ "         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1996>\" "
			+ "         OR [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].currentmember.name = \"<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>\" "
			+ "))) ON ROWS "
			+ " FROM [" + kylinProjectName + "] ";
	}

	private String query10() {
		return "SELECT {[Measures].[LO_REVENUE]} ON COLUMNS, "
			+ "Crossjoin({[LO_CUSTKEY].[LO_CUSTKEYCITYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyCityUNITED-KI1>], [LO_CUSTKEY].[LO_CUSTKEYCITYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyCityUNITED-KI5>]}, Crossjoin({[LO_SUPPKEY].[LO_SUPPKEYCITYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyCityUNITED-KI1>], [LO_SUPPKEY].[LO_SUPPKEYCITYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyCityUNITED-KI5>]}, "
			+ "{[LO_ORDERDATE].[LO_ORDERDATEYEARMONTHLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateYearMonthDec1997>]})) ON ROWS "
			+ " FROM [" + kylinProjectName + "] ";
	}

	private String query11() {
		return "WITH MEMBER [Measures].[Profit] as '[Measures].[LO_REVENUE] - [Measures].[LO_SUPPLYCOST]' "
			+ "SELECT {[Measures].[Profit]} ON COLUMNS, "
			+ "CrossJoin([LO_CUSTKEY].[LO_CUSTKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyRegionAMERICA>].children, [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].members) ON ROWS "
			+ "FROM [" + kylinProjectName + "] "
			+ "WHERE CrossJoin({[LO_PARTKEY].[LO_PARTKEYMFGRLEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyMfgrMFGR-1>], [LO_PARTKEY].[LO_PARTKEYMFGRLEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyMfgrMFGR-2>]}, [LO_SUPPKEY].[LO_SUPPKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionAMERICA>]) ";
	}

	private String query12() {
		return "WITH MEMBER [Measures].[Profit] as '[Measures].[LO_REVENUE] - [Measures].[LO_SUPPLYCOST]' "
			+ "SELECT {[Measures].[Profit]} ON COLUMNS, "
			+ "  CrossJoin({[LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>], [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateYear1998>]}, "
			+ "  Crossjoin( "
			+ "    [LO_SUPPKEY].[LO_SUPPKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyRegionAMERICA>].children, "
			+ "    Filter([LO_PARTKEY].[lo_partkeyCodeList].[LO_PARTKEYCATEGORYLEVEL].members, "
			+ "      [LO_PARTKEY].[lo_partkeyCodeList].currentmember.parent.name = \"<http://lod2.eu/schemas/rdfh#lo_partkeyMfgrMFGR-1>\""
			+ "      OR [LO_PARTKEY].[lo_partkeyCodeList].currentmember.parent.name = \"<http://lod2.eu/schemas/rdfh#lo_partkeyMfgrMFGR-2>\")) "
			+ "  ) ON ROWS "
			+ "FROM [" + kylinProjectName + "] "
			+ "WHERE [LO_CUSTKEY].[LO_CUSTKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyRegionAMERICA>] ";
	}

	private String query13() {
		return "WITH MEMBER [Measures].[Profit] as '[Measures].[LO_REVENUE] - [Measures].[LO_SUPPLYCOST]' "
			+ "SELECT {[Measures].[Profit]} ON COLUMNS, "
			+ "CrossJoin({[LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateYear1997>], [LO_ORDERDATE].[LO_ORDERDATEYEARLEVEL].[<http://lod2.eu/schemas/rdfh#lo_orderdateYear1998>]}, Crossjoin([LO_SUPPKEY].[LO_SUPPKEYNATIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_suppkeyNationUNITED-STATES>].children, [LO_PARTKEY].[LO_PARTKEYCATEGORYLEVEL].[<http://lod2.eu/schemas/rdfh#lo_partkeyCategoryMFGR-14>].children)) ON ROWS "
			+ "FROM [" + kylinProjectName + "] "
			+ "WHERE [LO_CUSTKEY].[LO_CUSTKEYREGIONLEVEL].[<http://lod2.eu/schemas/rdfh#lo_custkeyRegionAMERICA>] "
			+ "";
	}

	private void writeLogFile(CellSet cellSet, int queryNumber, int iterationNumber, long executionTime) throws FileNotFoundException {
		PrintWriter writer = new PrintWriter("runMDXQuery"+queryNumber+"."+iterationNumber+".log");
		RectangularCellSetFormatter formatter = new RectangularCellSetFormatter(false);
		formatter.format(cellSet, writer);

		writer.println("Execution Time: " + executionTime);
		writer.println("");
		writer.flush();
		writer.close();
	}
}

package org.rdfqb2kylin.etl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.mdm.Attribute;
import org.rdfqb2kylin.mdm.Cube;
import org.rdfqb2kylin.mdm.Dimension;
import org.rdfqb2kylin.mdm.Fact;
import org.rdfqb2kylin.mdm.Hierarchy;
import org.rdfqb2kylin.mdm.Level;
import org.rdfqb2kylin.mdm.Measure;
import org.rdfqb2kylin.util.UriUtil;

/*
 * Hive Table Generator
 * - Creates external or / and optimized Hive tables (depends on config properties)
 * - Creates Star Schema after reading MDM information from triples table
 */
public class HiveTableGenerator {
	private Connection connection;
	private Cube cube;

	private final static Logger logger = Logger.getLogger(HiveTableGenerator.class);

	public HiveTableGenerator(Connection connection) {
		this.connection = connection;
	}

	public void setCube(Cube cube) {
		this.cube = cube;
	}

	/*
	 * Create Triples Table.
	 * If Property hive.use_parquet is true, use PARQUET instead of TEXTFILE
	 * If Property hive.use_partition is true, use Partition on Predicate column
	 *
	 * If one of the Properties is true, use Property hive.optimized_triples_table as Hive Table name
	 */
	public void createTriplesTable() {
		createExternalTriplesTable();

		if(Property.useCompression()) {
			createOptimizedTriplesTable();
		}
	}

	/*
	 * Create External Hive Table
	 * This is necessary to create a Hive Table on top of N-Triples RDF data
	 */
	private void createExternalTriplesTable() {
		try (Statement statement = connection.createStatement()) {
			logger.info("Create External Triples Table '" + Property.get("hive.triples_table") + "'...");

			statement.execute("DROP TABLE IF EXISTS " + Property.get("hive.triples_table"));
			statement.execute(createExternalTriplesTableQuery());
		} catch (SQLException e) {
			logger.error("Could not create External Triples Table '" + Property.get("hive.triples_table") + "'!");
			e.printStackTrace();
			System.exit(0);
		}
	}

	/*
	 * Create optimized Hive Table 'hive.optimized_triples_table' from 'hive.triples_table' Hive Table
	 */
	private void createOptimizedTriplesTable() {
		try (Statement statement = connection.createStatement()) {
			logger.info("Create Optimized Triples Table '" + Property.get("hive.optimized_triples_table") + "'...");

			statement.execute("DROP TABLE IF EXISTS " + Property.get("hive.optimized_triples_table"));
			statement.execute(createOptimizedTriplesTableQuery());
			statement.executeQuery("SET hive.exec.dynamic.partition.mode=nonstrict");
			statement.execute(loadTriplesIntoOptimizedTableQuery());
		} catch (SQLException e) {
			logger.error("Could not create Optimized Triples Table '" + Property.get("hive.optimized_triples_table") + "'!");
			e.printStackTrace();
			System.exit(0);
		}
	}

	/*
	 * Create Star Schema Dimension Table for every dimension found in DSD
	 */
	public void createDimensionTables() {
		logger.info("Create Star Schema Dimension Tables...");

		for (Dimension dimension : cube.getFact().getDimensions()) {
			logger.info("Create Dimension Table: '" + dimension.getTableName() + "'...");

			try (Statement statement = connection.createStatement()) {
				if(dimension.getHierarchies().size() == 1) {
					statement.execute("DROP TABLE IF EXISTS " + dimension.getTableName());
					statement.execute(createDimensionTableWithSingleHierarchyQuery(dimension));
				} else {
					statement.execute("DROP TABLE IF EXISTS " + dimension.getTableName());

					int i = 1;
					for(Hierarchy hierarchy : dimension.getHierarchies()) {
						statement.execute("DROP TABLE IF EXISTS " + getStageDimensionName(dimension, hierarchy, i));
						statement.execute(createDimensionTableWithMultipleHierarchyQuery(dimension, hierarchy, i));
						i++;
					}
				}
			} catch (SQLException e) {
				logger.error("Could not create Dimension Table '" + dimension.getTableName() + "'!");
				e.printStackTrace();
				System.exit(0);
			}
		}
	}

	/*
	 * Create Star Schema Fact Table with every measure and foreign key found in DSD
	 */
	public void createFactTable() {
		logger.info("Create Hive Fact Table '" + Property.get("hive.fact_table_name") + "'...");

		Fact fact = cube.getFact();
		try (Statement statement = connection.createStatement()) {
			statement.execute("DROP TABLE IF EXISTS " + Property.get("hive.fact_table_name"));
			statement.execute(createFactTableQuery(fact));
		} catch (SQLException e) {
			logger.error("Could not create Fact Table '" + Property.get("hive.fact_table_name") + "'!");
			e.printStackTrace();
			System.exit(0);
		}
	}

	// Query for creating External Hive Triples Table
	private static String createExternalTriplesTableQuery() {
		String query = "CREATE EXTERNAL TABLE " + Property.get("hive.triples_table")
			+ " ( subject STRING, predicate STRING, object STRING ) "
			+ " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' "
			+ " WITH SERDEPROPERTIES ( \"input.regex\" = \"([^ ]*) ([^ ]*) (.*) \\.\",  \"output.format.string\" = \"%1$s %2$s %3$s\" " + ") "
			+ " STORED AS TEXTFILE "
			+ " LOCATION '" + Property.get("hdfs.triples_directory") + "' ";
		logger.debug("- createExternalTriplesTable Query: " + query);
		return query;
	}

	// Query for creating Optimized Hive Triples Table
	private static String createOptimizedTriplesTableQuery() {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE " + Property.get("hive.optimized_triples_table") + " ");

		if(Property.usePartition()) {
			sb.append("( subject STRING, object STRING ) ");
			sb.append("PARTITIONED BY (predicate String) ");
		} else {
			sb.append("( subject STRING, predicate String, object STRING ) ");
		}

		if(Property.useParquet()) {
			sb.append("STORED AS PARQUET");
		} else {
			sb.append("STORED AS TEXTFILE");
		}

		logger.debug("- createOptimizedTriplesTable Query: " + sb);
		return sb.toString();
	}

	// Query to insert triples into optimized Hive Triples Table
	private static String loadTriplesIntoOptimizedTableQuery() {
		StringBuilder sb = new StringBuilder();

		sb.append("INSERT INTO TABLE " + Property.get("hive.optimized_triples_table") + " ");

		if(Property.usePartition()) {
			sb.append("PARTITION (predicate) ");
			sb.append("SELECT subject, object, predicate ");
		} else {
			sb.append("SELECT subject, predicate, object ");
		}

		sb.append("FROM " + Property.get("hive.triples_table"));

		logger.debug("- loadTriplesToOptimizedTable Query: " + sb);
		return sb.toString();
	}

	// Query for creating Star Schema Dimension with one Hierarchy
	private String createDimensionTableWithSingleHierarchyQuery(Dimension dimension) {
		Hierarchy hierarchy = dimension.getHierarchies().get(0);
		StringBuilder sb = new StringBuilder();
		String prevColumn = "object";
		int i;

		sb.append("CREATE TABLE " + dimension.getTableName() + " ");
		sb.append("STORED AS PARQUET AS SELECT ");
		sb.append("  tb1.object AS key ");

		// Add Hierarchy Columns
		for(Level level : hierarchy.getLevels()) {
			if(hierarchy.getDeepestLevel() == level) {
				continue;
			}

			i = hierarchy.getLevels().size()-level.getDepth() + 1;

			sb.append(",  tb" + i + ".subject AS " + level.getName() + " ");
		}

		i = hierarchy.getLevels().size() + 1;

		// Add Attributes Columns, depends on Property global.loadMDMAttributes
		for(Attribute attribute : dimension.getAttributes()) {
			String casting = "cast(regexp_replace(split(tb" + i + ".object, \"\\\\^\\\\^\")[0],'\"','') as " + attribute.getType() + ")";
			sb.append(", " + casting + " " + attribute.getColumnName() + " ");

			i++;
		}

		sb.append("FROM " + Property.getQBTriplesName() + " tb1 ");

		i = 2;
		for(Level level : hierarchy.getLevels()) {
			if(hierarchy.getDeepestLevel() == level) {
				continue;
			}

			sb.append("LEFT JOIN " + Property.getQBTriplesName() + " tb" + i + " ");
			sb.append("ON (tb" + i + ".object = tb" + (i-1) + "." + prevColumn + " AND tb" + i + ".predicate = \"" + UriUtil.build("skos", "narrower") + "\") ");

			prevColumn = "subject";
			i++;
		}

		// Add LEFT JOIN conditions for Attribute Columns
		for(Attribute attribute : dimension.getAttributes()) {
			sb.append("LEFT JOIN " + Property.getQBTriplesName() + " tb" + i + " ");
			sb.append("ON (tb" + i + ".subject = tb1.object AND tb" + i + ".predicate = \"" + attribute.getUri() + "\") ");

			i++;
		}

		sb.append("WHERE ");
		sb.append("  tb1.subject = \"" + hierarchy.getDeepestLevel().getUri() + "\" ");
		sb.append("  AND tb1.predicate = \"" + UriUtil.build("skos", "member") + "\" ");

		logger.debug("- createDimensionTableWithSingleHierarchy Query: " + sb);

		return sb.toString();
	}

	// Query for creating Star Schema Dimension with more then one Hierarchy
	private String createDimensionTableWithMultipleHierarchyQuery(Dimension dimension, Hierarchy hierarchy, int stage) {
		StringBuilder sb = new StringBuilder();
		String prevColumn = "object";
		String fromTable = Property.getQBTriplesName();
		int i;

		sb.append("CREATE TABLE " + getStageDimensionName(dimension, hierarchy, stage) + " ");
		sb.append("STORED AS PARQUET AS SELECT ");

		if(stage == 1) {
			sb.append("  tb1.object as key ");
		} else if(stage == dimension.getHierarchies().size()) {
			fromTable = dimension.getTableName() + "_tmp_" + (stage-1);
			sb.append("  tb0.* ");
		}

		for(Level level : hierarchy.getLevels()) {
			if(hierarchy.getDeepestLevel() == level) {
				continue;
			}

			i = hierarchy.getLevels().size()-level.getDepth() + 1;

			sb.append(",  tb" + i + ".subject AS " + level.getName() + " ");
		}

		// Add Attributes Columns, depends on Property global.loadMDMAttributes and current stage
		if(stage == 1) {
			i = hierarchy.getLevels().size() + 1;

			for(Attribute attribute : dimension.getAttributes()) {
				String casting = "cast(regexp_replace(split(tb" + i + ".object, \"\\\\^\\\\^\")[0],'\"','') as " + attribute.getType() + ")";
				sb.append(", " + casting + " " + attribute.getColumnName() + " ");

				i++;
			}
		}

		sb.append("FROM " + Property.getQBTriplesName()  + " tb1 ");

		i = 2;
		for(Level level : hierarchy.getLevels()) {
			if(hierarchy.getDeepestLevel() == level) {
				continue;
			}

			sb.append("LEFT JOIN " + Property.getQBTriplesName() + " tb" + i + " ");
			sb.append("ON (tb" + i + ".object = tb" + (i-1) + "." + prevColumn + " AND tb" + i + ".predicate = \"" + UriUtil.build("skos", "narrower") + "\") ");

			prevColumn = "subject";
			i++;
		}

		if(stage > 1) {
			sb.append("JOIN " + fromTable + " as tb0 ON (tb1.object = tb0.key) ");
		} else {
			// Add LEFT JOIN conditions for Attribute Columns
			for(Attribute attribute : dimension.getAttributes()) {
				sb.append("LEFT JOIN " + Property.getQBTriplesName() + " tb" + i + " ");
				sb.append("ON (tb" + i + ".subject = tb1.object AND tb" + i + ".predicate = \"" + attribute.getUri() + "\") ");

				i++;
			}
		}

		sb.append("WHERE ");
		sb.append("  tb1.subject = \"" + hierarchy.getDeepestLevel().getUri() + "\" ");
		sb.append("  AND tb1.predicate = \"" + UriUtil.build("skos", "member") + "\" ");

		sb.append("  AND tb2.subject IN ( ");
		sb.append("    SELECT tbx.object ");
		sb.append("    FROM " + Property.getQBTriplesName() + " tbx ");
		sb.append("    WHERE ");
		sb.append("      tbx.subject = \"" + hierarchy.getLevels().get(hierarchy.getLevels().size()-2).getUri() + "\" ");
		sb.append("      AND tbx.predicate = \"<http://www.w3.org/2004/02/skos/core#member>\" ");
		sb.append("  )");

		logger.debug("- createDimensionTableWithMultipleHierarchy Query: " + sb);

		return sb.toString();
	}

	// Query for creating Star Schema Fact Table
	private String createFactTableQuery(Fact fact) {
		HashMap<String, String> measureMapping = new HashMap<String, String>();
		StringBuilder sb = new StringBuilder();
		int i = 2;

		sb.append("CREATE TABLE " + Property.get("hive.fact_table_name") + " ");
		sb.append("STORED AS PARQUET AS SELECT ");
		sb.append("  tb1.subject as key ");

		for(Dimension dimension : fact.getDimensions()) {
			sb.append(",  tb" + i + ".object as " + dimension.getTableName() + " ");
			i++;
		}

		for(Measure measure : fact.getMeasures()) {
			if(measure.isCalculatedMember()) {
				continue;
			}

			String casting = "cast(regexp_replace(split(tb" + i + ".object, \"\\\\^\\\\^\")[0],'\"','') as " + measure.getType() + ")";
			sb.append(", " + casting + " " + measure.getColumnName() + " ");
			measureMapping.put(measure.getUri(), casting);
			i++;
		}

		// Calculated Members
		for(Measure measure : fact.getMeasures()) {
			if(!measure.hasToBeCalculated()) {
				continue;
			}

			// Precalculate multiplication or division
			sb.append(",  cast(" + measureMapping.get(measure.getCalculatedMemberFormula()[0].getUri()) + "" + measure.getMathSign() + "" + measureMapping.get(measure.getCalculatedMemberFormula()[1].getUri()) + " as " + measure.getType() + ") " + measure.getColumnName() + " ");
		}

		sb.append("FROM " + Property.getQBTriplesName() + " tb1 ");

		i = 2;

		for(Dimension dimension : fact.getDimensions()) {
			sb.append("LEFT JOIN " + Property.getQBTriplesName() + " tb" + i +" ");
			sb.append("ON ( tb" + i + ".subject = tb1.subject AND tb" + i + ".predicate = \"" + dimension.getUri() + "\" ) ");
			i++;
		}

		for(Measure measure : fact.getMeasures()) {
			if(measure.isCalculatedMember()) {
				// Nothing to do for calculated members here...
				continue;
			}

			sb.append("LEFT JOIN " + Property.getQBTriplesName() + " tb" + i +" ");
			sb.append("ON ( tb" + i + ".subject = tb1.subject AND tb" + i + ".predicate = \"" + measure.getUri() + "\" ) ");
			i++;
		}

		sb.append("WHERE ");
		sb.append("  tb1.object = \"" + UriUtil.build("qb", "Observation") + "\" ");
		sb.append("  AND tb1.predicate = \"" + UriUtil.build("rdf", "type") + "\" ");

		logger.debug("- createFactTable Query: " + sb);

		return sb.toString();
	}

	// Get Star Schema Temporary Dimension Name
	private String getStageDimensionName(Dimension dimension, Hierarchy hierarchy, int stage) {
		String tableName = dimension.getTableName();
		if(dimension.getLastHierarchy() != hierarchy) {
			tableName = dimension.getTableName() + "_tmp_" + stage;
		}

		return tableName;
	}
}

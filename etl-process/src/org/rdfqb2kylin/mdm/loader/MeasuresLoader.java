package org.rdfqb2kylin.mdm.loader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.etl.Property;
import org.rdfqb2kylin.mdm.Measure;
import org.rdfqb2kylin.util.UriUtil;

/*
 * MDM Loader - Measure Loader Class
 * Loads measures from Hive Table with all N-Triples.
 */
public class MeasuresLoader {
	private Connection connection;
	private HashMap<String, Measure> measures;
	private HashMap<String, String> componentsMapping;

	private final static Logger logger = Logger.getLogger(MeasuresLoader.class);

	public MeasuresLoader(Connection connection) {
		this.connection = connection;
		this.measures = new HashMap<String, Measure>();
		this.componentsMapping = new HashMap<String, String>();

		load();
	}

	private void load() {
		logger.info("Loading all Measures...");

		try (Statement statement = connection.createStatement()) {
			// Load all Measures with qb:measure Property
			try (ResultSet resultSet = statement.executeQuery(queryMeasures())) {
				while (resultSet.next()) {
					String componentUri = resultSet.getString("tb1.subject");
					String measureUri = resultSet.getString("tb1.object");

					Measure measure = new Measure();
					measure.setComponentUri(componentUri);
					measure.setUri(measureUri);
					measure.setColumnName(UriUtil.clean(measureUri).toUpperCase());

					measures.put(measureUri, measure);
					componentsMapping.put(componentUri, measureUri);
				}
			}

			// Load all Aggregation Functions with qb4o:aggregateFunction
			// Property
			try (ResultSet resultSet = statement.executeQuery(queryAggregationFunctions())) {
				while (resultSet.next()) {
					String componentUri = resultSet.getString("tb1.subject");
					String aggregationFunction = resultSet.getString("tb1.object");

					Measure measure = measures.get(componentsMapping.get(componentUri));

					if (UriUtil.isUri(aggregationFunction)) {
						measure.setAggregator(UriUtil.convertToAggregator(aggregationFunction));
					} else {
						measure.setIsCalculatedMember(true);

						// Calculated Members with * or / has to be precomputed
						// in starschema for interaction with Kylin
						// Calculated Members with + or - has not to be computed
						String formula = aggregationFunction.trim();

						// Currently only sum is supported!
						// Formular must have syntax sum(prefix:measure1 *
						// prefix:measure2)
						formula = formula.replace("\"", "");
						formula = formula.replace("sum", "");
						formula = formula.replace("(", "").replace(")", "");

						String component1 = "";
						String component2 = "";

						if (formula.contains("+")) {
							measure.setMathSign("+");
							String[] components = formula.split("\\+");
							component1 = components[0];
							component2 = components[1];
						} else if (formula.contains("-")) {
							measure.setMathSign("-");
							String[] components = formula.split("\\-");
							component1 = components[0];
							component2 = components[1];
						} else if (formula.contains("*")) {
							measure.setMathSign("*");
							String[] components = formula.split("\\*");
							component1 = components[0];
							component2 = components[1];
						} else if (formula.contains("/")) {
							measure.setMathSign("/");
							String[] components = formula.split("/");
							component1 = components[0];
							component2 = components[1];
						}

						String[] component1Value = component1.split(":");
						String[] component2Value = component2.split(":");

						String measure1Uri = UriUtil.build(component1Value[0].trim(), component1Value[1].trim());
						String measure2Uri = UriUtil.build(component2Value[0].trim(), component2Value[1].trim());

						Measure measure1 = measures.get(measure1Uri);
						Measure measure2 = measures.get(measure2Uri);

						if (measure1 == null || measure2 == null) {
							logger.warn("Calculated Member '" + measure.getUri() + "' could not be calculated correctly...");
							measures.remove(measure.getUri());
							continue;
						}

						measure.setCalculatedMemberComponents(measure1, measure2);
					}

				}
			}

			// Load all Measure Information
			try (ResultSet resultSet = statement.executeQuery(queryMeasureInformation())) {
				while (resultSet.next()) {
					String measureUri = resultSet.getString("tb1.subject");
					String predicate = resultSet.getString("tb1.predicate");
					String object = resultSet.getString("tb1.object");

					if (predicate.equals(UriUtil.build("rdfs", "label"))) {
						measures.get(measureUri).setName(UriUtil.clean(object));
					} else if (predicate.equals(UriUtil.build("rdfs", "range"))) {
						measures.get(measureUri).setType(UriUtil.clean(object));
					}
				}
			}

		} catch (SQLException e) {
			logger.error("SQLException: " + e.getMessage());
			System.exit(0);
		}
	}

	private String queryMeasures() {
		StringBuilder sb = new StringBuilder();

		sb.append("SELECT tb1.* ");
		sb.append("FROM " + Property.getQBTriplesName() + " AS tb1 ");
		sb.append("WHERE predicate = \"" + UriUtil.build("qb", "measure") + "\" ");

		logger.debug("- load Measures Query: " + sb);
		return sb.toString();
	}

	private String queryAggregationFunctions() {
		StringBuilder sb = new StringBuilder();

		sb.append("SELECT tb1.* ");
		sb.append("FROM " + Property.getQBTriplesName() + " AS tb1 ");
		sb.append("WHERE predicate = \"" + UriUtil.build("qb4o", "aggregateFunction") + "\" ");

		logger.debug("- load Measure Aggregation Functions Query: " + sb);
		return sb.toString();
	}

	private String queryMeasureInformation() {
		StringBuilder sb = new StringBuilder();
		String delim = "";

		sb.append("SELECT tb1.* ");
		sb.append("FROM " + Property.getQBTriplesName() + " as tb1 ");
		sb.append("WHERE ");
		sb.append("  ( ");
		sb.append("    tb1.predicate = \"" + UriUtil.build("rdfs", "range") + "\" ");
		sb.append("    OR tb1.predicate = \"" + UriUtil.build("rdfs", "label") + "\" ");
		sb.append("  ) ");
		sb.append("  AND tb1.subject IN ( ");

		for (Measure measure : measures.values()) {
			sb.append(delim).append("\"" + measure.getUri() + "\" ");
			delim = ", ";
		}

		sb.append("  ) ");

		logger.debug("- load Measure Information Query: " + sb);
		return sb.toString();
	}

	public Collection<Measure> get() {
		return measures.values();
	}

	public HashMap<String, Measure> getList() {
		return measures;
	}
}

package org.rdfqb2kylin.mdm.loader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.etl.Property;
import org.rdfqb2kylin.mdm.Attribute;
import org.rdfqb2kylin.mdm.Dimension;
import org.rdfqb2kylin.mdm.Hierarchy;
import org.rdfqb2kylin.mdm.Level;
import org.rdfqb2kylin.util.StringUtil;
import org.rdfqb2kylin.util.UriUtil;

/*
 * MDM Loader - Dimension Loader Class
 * Loads dimension, hierarchies, levels and attributes from Hive Table with all N-Triples.
 */
public class DimensionsLoader {
	private Connection connection;
	private HashMap<String, Dimension> dimensions;
	private HashMap<String, Hierarchy> hierarchies;
	private HashMap<String, Level> levels;

	private final static Logger logger = Logger.getLogger(DimensionsLoader.class);

	public DimensionsLoader(Connection connection) {
		this.connection = connection;
		this.dimensions = new HashMap<String, Dimension>();
		this.hierarchies = new HashMap<String, Hierarchy>();
		this.levels = new HashMap<String, Level>();

		loadDimensions();
	}

	private void loadDimensions() {
		logger.info("Loading all Dimensions, Hierarchies and Levels...");

		try (Statement statement = connection.createStatement()) {
			// Load all Dimensions with qb:dimension Property
			try (ResultSet resultSet = statement.executeQuery(queryDimensions())) {
				while (resultSet.next()) {
					String dimensionUri = resultSet.getString("tb1.object");

					Dimension dimension = new Dimension();
					dimension.setUri(dimensionUri);
					dimension.setName(UriUtil.clean(dimensionUri));
					dimension.setTableName(UriUtil.clean(dimensionUri).toUpperCase());
					dimension.setPrimaryColumnName("KEY");

					dimensions.put(dimensionUri, dimension);
				}
			}

			// Load all Hierarchies with qb:codeList Property
			try (ResultSet resultSet = statement.executeQuery(queryHierarchies())) {
				while (resultSet.next()) {
					String dimensionUri = resultSet.getString("tb1.subject");
					String codeListUri = resultSet.getString("tb1.object");

					Hierarchy hierarchy = new Hierarchy();
					hierarchy.setUri(codeListUri);
					hierarchy.setName(UriUtil.clean(codeListUri));
					hierarchies.put(codeListUri, hierarchy);

					dimensions.get(dimensionUri).addHierarchy(hierarchy);
					hierarchy.setDimension(dimensions.get(dimensionUri));
				}
			}

			// Load all Levels
			try (ResultSet resultSet = statement.executeQuery(queryLevels())) {
				while (resultSet.next()) {
					String levelUri = resultSet.getString("tb1.subject");
					String codeListUri = resultSet.getString("tb1.object");
					String depth = resultSet.getString("depth");

					Level level = new Level();
					level.setUri(levelUri);
					level.setName(UriUtil.clean(levelUri).toUpperCase());
					level.setDepth(StringUtil.toInt(depth));
					levels.put(levelUri, level);

					hierarchies.get(codeListUri).addLevel(level);
					level.setHierarchy(hierarchies.get(codeListUri));
				}
			}

			// Optional: Load all Attributes
			if (Property.get("global.loadMDMAttributes").equals("true")) {
				try (ResultSet resultSet = statement.executeQuery(queryAttributes())) {
					while (resultSet.next()) {
						String levelUri = resultSet.getString("tb1.subject");
						String attributeUri = resultSet.getString("tb2.predicate");

						Attribute attribute = new Attribute();
						attribute.setUri(attributeUri);
						attribute.setName(UriUtil.clean(attributeUri));
						attribute.setColumnName(UriUtil.clean(attributeUri).toUpperCase());
						attribute.setLevel(levels.get(levelUri));
						attribute.setType("String");

						Dimension dimension = levels.get(levelUri).getHierarchy().getDimension();
						dimension.addAttribute(attribute);
					}
				}
			}
		} catch (SQLException e) {
			logger.error("SQLException: " + e.getMessage());
			System.exit(0);
		}
	}

	private String queryDimensions() {
		StringBuilder sb = new StringBuilder();

		sb.append("SELECT tb1.* ");
		sb.append("FROM " + Property.getQBTriplesName() + " AS tb1 ");
		sb.append("WHERE predicate = \"" + UriUtil.build("qb", "dimension") + "\" ");

		logger.debug("- load Dimensions Query: " + sb);
		return sb.toString();
	}

	private String queryHierarchies() {
		StringBuilder sb = new StringBuilder();

		sb.append("SELECT tb1.* ");
		sb.append("FROM " + Property.getQBTriplesName() + " AS tb1 ");
		sb.append("WHERE predicate = \"" + UriUtil.build("qb", "codeList") + "\" ");

		logger.debug("- load CodeLists Query: " + sb);
		return sb.toString();
	}

	private String queryLevels() {
		StringBuilder sb = new StringBuilder();

		sb.append("SELECT tb1.subject, tb1.object, tb2.object as depth ");
		sb.append("FROM " + Property.getQBTriplesName() + " as tb1 ");
		sb.append("JOIN " + Property.getQBTriplesName() + " as tb2 ON (tb1.subject = tb2.subject) ");
		sb.append("WHERE ");
		sb.append("  tb1.predicate = \"" + UriUtil.build("skos", "inScheme") + "\" ");
		sb.append("  AND tb2.predicate = \"" + UriUtil.build("xkos", "depth") + "\" ");

		logger.debug("- load Levels Query: " + sb);
		return sb.toString();
	}

	private String queryAttributes() {
		StringBuilder sb = new StringBuilder();
		String delim = "";

		sb.append("SELECT tb1.subject, tb2.predicate ");
		sb.append("FROM " + Property.getQBTriplesName() + " AS tb1 ");
		sb.append("JOIN " + Property.getQBTriplesName() + " AS tb2 ON (tb2.subject = tb1.object) ");
		sb.append("WHERE ");
		sb.append("  tb1.subject IN ( ");

		for (Entry<String, Hierarchy> entry : hierarchies.entrySet()) {
			Hierarchy hierarchy = entry.getValue();

			sb.append(delim).append("\"" + hierarchy.getDeepestLevel().getUri() + "\" ");
			delim = ", ";
		}

		sb.append("  ) ");
		sb.append("  AND tb1.predicate = \"<http://www.w3.org/2004/02/skos/core#member>\" ");
		sb.append("  AND tb2.predicate != \"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\" ");
		sb.append("  AND tb2.predicate != \"<http://www.w3.org/2004/02/skos/core#inScheme>\" ");
		sb.append("  GROUP BY tb1.subject, tb2.predicate ");

		logger.debug("- load Attributes Query: " + sb);
		return sb.toString();
	}

	public Collection<Dimension> get() {
		return dimensions.values();
	}

	public HashMap<String, Dimension> getList() {
		return dimensions;
	}
}

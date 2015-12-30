package org.rdfqb2kylin.kylin.api.model;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.rdfqb2kylin.etl.Property;
import org.rdfqb2kylin.mdm.Cube;
import org.rdfqb2kylin.mdm.Dimension;
import org.rdfqb2kylin.mdm.Hierarchy;
import org.rdfqb2kylin.mdm.Level;

public class BuildCubeDescRowKey {
	private Cube cube;

	public BuildCubeDescRowKey(Cube cube) {
		this.cube = cube;
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			ArrayList<String> columnNames = new ArrayList<String>();
			ArrayList<ArrayList<String>> aggregationGroups = new ArrayList<ArrayList<String>>();

			ArrayList<Dimension> factDimensions = new ArrayList<Dimension>();

			// Use Aggregation Groups. Every Level of a Dimension in one single Group.
			if(Property.get("kylin.useAggregationGroups").equals("true")) {
				for (Dimension dimension : cube.getDimensions()) {
					ArrayList<String> aggregationGroup = new ArrayList<String>();

					if (dimension.getAttributes().size() > 0) {
						columnNames.add(dimension.getTableName());
						aggregationGroup.add(dimension.getTableName());
					}

					if (dimension.getHierarchies().size() > 0) {
						for (Hierarchy hierarchy : dimension.getHierarchies()) {
							if (hierarchy.getLevels().size() == 0) {
								continue;
							}

							for (Level level : hierarchy.getLevels()) {
								if(hierarchy.getDeepestLevel() == level) {
									continue;
								}

								aggregationGroup.add(level.getName());
								columnNames.add(level.getName());
							}
						}
					}

					if(aggregationGroup.size() > 0) {
						aggregationGroups.add(aggregationGroup);
					}

					// Dimension from Fact Table in one Aggregation Group
					if(dimension.getHierarchies().size() == 0 && dimension.getAttributes().size() == 0) {
						factDimensions.add(dimension);
						columnNames.add(dimension.getTableName());
					}
				}

				ArrayList<String> aggregationFactDimensionGroup = new ArrayList<String>();
				for(Dimension dimension : factDimensions) {
					aggregationFactDimensionGroup.add(dimension.getTableName());
				}
				aggregationGroups.add(aggregationFactDimensionGroup);

			// Do not use Aggregation Groups.
			} else {
				ArrayList<String> aggregationGroup = new ArrayList<String>();

				for (Dimension dimension : cube.getDimensions()) {
					if (dimension.getAttributes().size() > 0) {
						columnNames.add(dimension.getTableName());
						aggregationGroup.add(dimension.getTableName());
					}

					if (dimension.getHierarchies().size() > 0) {
						for (Hierarchy hierarchy : dimension.getHierarchies()) {
							if (hierarchy.getLevels().size() == 0) {
								continue;
							}

							for (Level level : hierarchy.getLevels()) {
								if(hierarchy.getDeepestLevel() == level) {
									continue;
								}

								aggregationGroup.add(level.getName());
								columnNames.add(level.getName());
							}
						}
					// Dimension from Fact Table in one Aggregation Group
					} else if(dimension.getHierarchies().size() == 0 && dimension.getAttributes().size() == 0) {
						aggregationGroup.add(dimension.getTableName());
						columnNames.add(dimension.getTableName());
					}
				}

				aggregationGroups.add(aggregationGroup);
			}


			// Aggregation Groups
			JSONArray jsonAggregationGroups = new JSONArray();
			for (ArrayList<String> aggregationGroup : aggregationGroups) {
				jsonAggregationGroups.put(new JSONArray(aggregationGroup));
			}

			// Row Key Columns
			JSONArray jsonRowKeyColumns = new JSONArray();
			for (String columnName : columnNames) {
				JSONObject rowKeyColumn = new JSONObject();
				rowKeyColumn.put("column", columnName);
				rowKeyColumn.put("dictionary", "true");
				rowKeyColumn.put("length", 0);
				rowKeyColumn.put("mandatory", false);

				jsonRowKeyColumns.put(rowKeyColumn);
			}

			obj.put("aggregation_groups", jsonAggregationGroups);
			obj.put("rowkey_columns", jsonRowKeyColumns);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

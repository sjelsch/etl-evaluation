package org.rdfqb2kylin.kylin.api.model;

import java.util.ArrayList;

import org.json.JSONArray;
import org.rdfqb2kylin.mdm.Attribute;
import org.rdfqb2kylin.mdm.Cube;
import org.rdfqb2kylin.mdm.Dimension;
import org.rdfqb2kylin.mdm.Hierarchy;
import org.rdfqb2kylin.mdm.Level;

public class BuildCubeDescDimensions {
	private Cube cube;

	private ArrayList<BuildCubeDescDimension> cubeDescDimensions;

	public BuildCubeDescDimensions(Cube cube) {
		this.cube = cube;
		this.cubeDescDimensions = new ArrayList<BuildCubeDescDimension>();

		loadDimensions();
	}

	private void loadDimensions() {
		int id = 1;

		for (Dimension dimension : cube.getDimensions()) {
			// Add Derived Attributes
			if (dimension.getAttributes().size() > 0) {
				ArrayList<Attribute> attributes = dimension.getAttributes();

				ArrayList<String> derivedNames = new ArrayList<String>();
				for ( Attribute attribute : attributes ) {
					derivedNames.add(attribute.getColumnName());
				}

				BuildCubeDescDimension cubeDescDimension = new BuildCubeDescDimension();
				cubeDescDimension.setName(dimension.getTableName() + "_Derived_" + id);
				cubeDescDimension.setTable("DEFAULT." + dimension.getTableName());
				cubeDescDimension.setID(id);
				cubeDescDimension.setHasHierarchy(false);
				cubeDescDimension.setDerivations(derivedNames.toArray(new String[derivedNames.size()]));
				cubeDescDimensions.add(cubeDescDimension);
				id++;
			}

			// Add Hierarchies
			if (dimension.getHierarchies().size() > 0) {
				for (Hierarchy hierarchy : dimension.getHierarchies()) {
					if(hierarchy.getLevels().size() == 0) {
						continue;
					}

					ArrayList<String> columnNames = new ArrayList<String>();

					for(Level level : hierarchy.getLevels()) {
						columnNames.add(level.getName());
					}
					columnNames.remove(columnNames.size()-1);

					BuildCubeDescDimension cubeDescDimension = new BuildCubeDescDimension();
					cubeDescDimension.setName(dimension.getTableName() + "_Hierarchy_" + id);
					cubeDescDimension.setTable("DEFAULT." + dimension.getTableName());
					cubeDescDimension.setID(id);
					cubeDescDimension.setHasHierarchy(true);
					cubeDescDimension.setColumns(columnNames.toArray(new String[columnNames.size()]));

					cubeDescDimensions.add(cubeDescDimension);
					id++;
				}

			// Dimension from Fact Table
			} else if(dimension.getHierarchies().size() == 0 && dimension.getAttributes().size() == 0) {
				String columnNames[] = {dimension.getTableName()};

				BuildCubeDescDimension cubeDescDimension = new BuildCubeDescDimension();
				cubeDescDimension.setName(dimension.getTableName() + "_Normal_" + id);
				cubeDescDimension.setTable("DEFAULT." + cube.getFact().getTableName());
				cubeDescDimension.setID(id);
				cubeDescDimension.setDerivations(null);
				cubeDescDimension.setHasHierarchy(false);
				cubeDescDimension.setColumns(columnNames);
				cubeDescDimensions.add(cubeDescDimension);
				id++;
			}
		}
	}

	public JSONArray toJSON() {
		JSONArray objs = new JSONArray();

		for (BuildCubeDescDimension cubeDescDimension : cubeDescDimensions) {
			objs.put(cubeDescDimension.toJSON());
		}

		return objs;
	}
}

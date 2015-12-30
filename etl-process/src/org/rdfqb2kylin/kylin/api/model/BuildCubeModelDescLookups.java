package org.rdfqb2kylin.kylin.api.model;

import java.util.ArrayList;

import org.json.JSONArray;
import org.rdfqb2kylin.mdm.Cube;
import org.rdfqb2kylin.mdm.Dimension;

public class BuildCubeModelDescLookups {
	private Cube cube;
	private ArrayList<BuildCubeModelDescLookup> lookups;

	public BuildCubeModelDescLookups(Cube cube) {
		this.cube = cube;

		this.lookups = new ArrayList<BuildCubeModelDescLookup>();

		loadLookups();
	}

	private void loadLookups() {
		for (Dimension dimension : cube.getFact().getDimensions()) {
			String[] foreignKeys = { dimension.getTableName() };
			String[] primaryKeys = { "KEY" };

			BuildCubeModelDescLookup lookup = new BuildCubeModelDescLookup();
			lookup.setTable("DEFAULT." + dimension.getTableName());
			lookup.setJoinForeignKeys(foreignKeys);
			lookup.setJoinPrimaryKeys(primaryKeys);
			lookup.setJoinType("inner");

			lookups.add(lookup);
		}
	}

	public JSONArray toJSON() {
		JSONArray objs = new JSONArray();

		for (BuildCubeModelDescLookup lookup : lookups) {
			objs.put(lookup.toJSON());
		}

		return objs;
	}
}

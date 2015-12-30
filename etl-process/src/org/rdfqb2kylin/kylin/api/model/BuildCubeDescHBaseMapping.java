package org.rdfqb2kylin.kylin.api.model;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.rdfqb2kylin.mdm.Cube;
import org.rdfqb2kylin.mdm.Measure;

public class BuildCubeDescHBaseMapping {
	private Cube cube;

	public BuildCubeDescHBaseMapping(Cube cube) {
		this.cube = cube;
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		JSONArray jsonColumnFamilies = new JSONArray();
		JSONObject jsonColumnFamily = new JSONObject();

		JSONArray jsonColumns = new JSONArray();
		JSONObject jsonColumn = new JSONObject();

		try {
			jsonColumnFamily.put("name", "f1");
			jsonColumn.put("qualifier", "m");
			jsonColumn.put("measure_refs", measureNames());
			jsonColumns.put(jsonColumn);
			jsonColumnFamily.put("columns", jsonColumns);
			jsonColumnFamilies.put(jsonColumnFamily);
			obj.put("column_family", jsonColumnFamilies);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}

	private ArrayList<String> measureNames() {
		ArrayList<String> measureNames = new ArrayList<String>();

		measureNames.add("_COUNT_");

		for (Measure measure : cube.getMeasures()) {
			if(measure.isCalculatedMember() && !measure.hasToBeCalculated()) {
				continue;
			}

			String measureName = measure.getColumnName() + "_" + measure.getAggregator();
			measureNames.add(measureName);
		}

		return measureNames;
	}
}

package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;

public class BuildCubeDescDimension {
	private String name;
	private String table;

	private int id;

	private String[] columns;
	private String[] derivations;

	private boolean hasHierarchy;

	public BuildCubeDescDimension() {

	}

	public void setName(String name) {
		this.name = name;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setID(int id) {
		this.id = id;
	}

	public void setHasHierarchy(boolean hasHierarchy) {
		this.hasHierarchy = hasHierarchy;
	}

	public void setColumns(String[] columns) {
		this.columns = columns;
	}

	public void setDerivations(String[] derivations) {
		this.derivations = derivations;
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("name", name);
			obj.put("table", table);
			obj.put("id", id);
			obj.put("hierarchy", hasHierarchy);

			if (columns != null) {
				obj.put("column", columns);
			} else {
				obj.put("column", JSONObject.NULL);
			}

			if (derivations != null) {
				obj.put("derived", derivations);
			} else {
				obj.put("derived", JSONObject.NULL);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

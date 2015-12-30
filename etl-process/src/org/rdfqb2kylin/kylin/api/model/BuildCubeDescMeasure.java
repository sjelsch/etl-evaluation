package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;

public class BuildCubeDescMeasure {
	private String name;
	private int id;

	private BuildCubeDescMeasureFunction cubeDescMeasureFunction;

	public BuildCubeDescMeasure() {

	}

	public void setName(String name) {
		this.name = name;
	}

	public void setID(int id) {
		this.id = id;
	}

	public void setFunction(BuildCubeDescMeasureFunction cubeDescMeasureFunction) {
		this.cubeDescMeasureFunction = cubeDescMeasureFunction;
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("name", name);
			obj.put("id", id);
			obj.put("function", cubeDescMeasureFunction.toJSON());
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

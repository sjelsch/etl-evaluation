package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;

public class CreateKylinCube {
	private String buildType;
	private int startTime;
	private int endTime;

	public CreateKylinCube() {
		this.buildType = "BUILD";
		this.startTime = 0;
		this.endTime = 0;
	}

	// Create JSON Object for creating OLAP Cube in Kylin
	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("buildType", buildType);
			obj.put("startTime", startTime);
			obj.put("endTime", endTime);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

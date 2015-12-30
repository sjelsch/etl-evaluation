package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;
import org.rdfqb2kylin.etl.Kylin;

public class CreateKylinProject {
	private Kylin kylin;

	private String name;
	private String description;

	public CreateKylinProject(Kylin kylin) {
		this.kylin = kylin;

		this.name = this.kylin.getProjectName();
		this.description = "Project for SSB RDF QB Data.";
	}

	// Create JSON Object for creating Kylin Project
	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("name", name);
			obj.put("description", description);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

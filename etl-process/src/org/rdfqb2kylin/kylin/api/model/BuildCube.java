package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;
import org.rdfqb2kylin.etl.Kylin;

public class BuildCube {
	private Kylin kylin;

	private String project;

	private BuildCubeDesc cubeDesc;
	private BuildCubeModelDesc modelDesc;

	public BuildCube(Kylin kylin) {
		this.kylin = kylin;

		project = kylin.getProjectName();

		cubeDesc = new BuildCubeDesc(this.kylin);
		modelDesc = new BuildCubeModelDesc(this.kylin);
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("project", project);
			obj.put("cubeDescData", cubeDesc.toJSON().toString());
			obj.put("modelDescData", modelDesc.toJSON().toString());
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

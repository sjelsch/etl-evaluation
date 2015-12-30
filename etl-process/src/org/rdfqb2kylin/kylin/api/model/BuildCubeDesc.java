package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;
import org.rdfqb2kylin.etl.Kylin;

/*
 * Kylins Cube Model needs two important JSON Keys:
 * - CubeDesc
 * - ModelDesc
 *
 * This Class represents CubeDesc information and generates all necessary JSON keys and values
 */
public class BuildCubeDesc {
	private String name;
	private String modelName;
	private String project;
	private String description;
	private String capacity;
	private String[] notifyList;

	private BuildCubeDescDimensions dimensions;
	private BuildCubeDescMeasures measures;
	private BuildCubeDescHBaseMapping hbaseMapping;
	private BuildCubeDescRowKey rowKey;

	public BuildCubeDesc(Kylin kylin) {
		this.name = kylin.getCubeModelName();
		this.modelName = kylin.getCubeModelName();
		this.project = kylin.getProjectName();
		this.description = "Project: " + project + " - Cube Model: " + modelName;
		this.notifyList = new String[0];
		this.capacity = "";

		this.dimensions = new BuildCubeDescDimensions(kylin.getCube());
		this.measures = new BuildCubeDescMeasures(kylin.getCube());
		this.hbaseMapping = new BuildCubeDescHBaseMapping(kylin.getCube());
		this.rowKey = new BuildCubeDescRowKey(kylin.getCube());
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("name", name);
			obj.put("model_name", modelName);
			obj.put("project", project);
			obj.put("description", description);
			obj.put("notify_list", notifyList);
			obj.put("capacity", capacity);

			obj.put("dimensions", dimensions.toJSON());
			obj.put("measures", measures.toJSON());
			obj.put("hbase_mapping", hbaseMapping.toJSON());
			obj.put("rowkey", rowKey.toJSON());
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

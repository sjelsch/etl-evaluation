package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;
import org.rdfqb2kylin.etl.Kylin;

/*
 * Kylins Cube Model needs two important JSON Keys:
 * - CubeDesc
 * - ModelDesc
 *
 * This Class represents ModelDesc information and generates all necessary JSON keys and values
 */
public class BuildCubeModelDesc {
	private Kylin kylin;

	private String name;
	private String capacity;
	private String factTable;
	private String filterCondition;

	private int lastModified;

	private BuildCubeModelDescPartition partitionDesc;
	private BuildCubeModelDescLookups lookups;

	public BuildCubeModelDesc(Kylin kylin) {
		this.kylin = kylin;

		this.name = kylin.getCubeModelName();
		this.capacity = "SMALL";
		this.factTable = "DEFAULT." + this.kylin.getCube().getFact().getTableName();
		this.filterCondition = "";
		this.lastModified = 0;

		this.lookups = new BuildCubeModelDescLookups(kylin.getCube());
		this.partitionDesc = new BuildCubeModelDescPartition();
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("name", name);
			obj.put("capacity", capacity);
			obj.put("fact_table", factTable);
			obj.put("filter_condition", filterCondition);
			obj.put("last_modified", lastModified);

			obj.put("lookups", lookups.toJSON());
			obj.put("partition_desc", partitionDesc.toJSON());
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

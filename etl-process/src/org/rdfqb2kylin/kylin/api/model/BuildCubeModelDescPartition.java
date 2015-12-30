package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;

public class BuildCubeModelDescPartition {
	private String partitionDateColumn;
	private String partitionType;

	private int partitionDateStart;

	public BuildCubeModelDescPartition() {
		this.partitionDateColumn = "";
		this.partitionType = "APPEND";
		this.partitionDateStart = 0;
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("partition_date_column", partitionDateColumn);
			obj.put("partition_type", partitionType);

			obj.put("partition_date_start", partitionDateStart);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

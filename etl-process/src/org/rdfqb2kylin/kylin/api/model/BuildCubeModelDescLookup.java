package org.rdfqb2kylin.kylin.api.model;

import org.json.JSONException;
import org.json.JSONObject;

public class BuildCubeModelDescLookup {
	private String table;
	private String joinType;

	private String[] primaryKeys;
	private String[] foreignKeys;

	public BuildCubeModelDescLookup() {

	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public void setJoinPrimaryKeys(String[] primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public void setJoinForeignKeys(String[] foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();
		JSONObject joinObj = new JSONObject();

		try {
			joinObj.put("type", joinType);
			joinObj.put("foreign_key", foreignKeys);
			joinObj.put("primary_key", primaryKeys);

			obj.put("table", table);
			obj.put("join", joinObj);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

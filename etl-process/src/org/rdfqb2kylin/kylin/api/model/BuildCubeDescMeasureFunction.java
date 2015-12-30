package org.rdfqb2kylin.kylin.api.model;

import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

public class BuildCubeDescMeasureFunction {
	private String expression;
	private String returnType;

	private HashMap<String, String> parameters;

	public BuildCubeDescMeasureFunction() {
		parameters = new HashMap<String, String>();
	}

	public void setExpression(String expression) {
		this.expression = expression.toUpperCase();
	}

	public void setReturnType(String returnType) {
		this.returnType = returnType;
	}

	public void setParameters(String type, String value) {
		parameters.put("type", type);
		parameters.put("value", value);
	}

	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("expression", expression);
			obj.put("parameter", parameters);
			obj.put("returntype", returnType);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return obj;
	}
}

package org.rdfqb2kylin.kylin.api.model;

import java.util.ArrayList;

import org.json.JSONArray;
import org.rdfqb2kylin.etl.Kylin;
import org.rdfqb2kylin.mdm.Cube;
import org.rdfqb2kylin.mdm.Measure;
import org.rdfqb2kylin.util.UriUtil;

public class BuildCubeDescMeasures {
	private Cube cube;

	private ArrayList<BuildCubeDescMeasure> cubeDescMeasures;

	public BuildCubeDescMeasures(Cube cube) {
		this.cube = cube;
		this.cubeDescMeasures = new ArrayList<BuildCubeDescMeasure>();

		loadCountMeasure();
		loadMeasures();
	}

	private void loadMeasures() {
		int n = 2;
		for (Measure measure : cube.getFact().getMeasures()) {
			if(!measure.isCalculatedMember() || measure.hasToBeCalculated()) {
				String measureName = UriUtil.clean(measure.getUri()) + "_" + measure.getAggregator();

				BuildCubeDescMeasure cubeDescMeasure = new BuildCubeDescMeasure();
				cubeDescMeasure.setName(measureName);
				cubeDescMeasure.setID(n);

				BuildCubeDescMeasureFunction cubeDescMeasureFunction = new BuildCubeDescMeasureFunction();
				cubeDescMeasureFunction.setExpression(measure.getAggregator());
				cubeDescMeasureFunction.setReturnType(Kylin.getMeasureReturnType(measure.getType()));
				cubeDescMeasureFunction.setParameters("column", UriUtil.clean(measure.getUri()).toUpperCase());

				cubeDescMeasure.setFunction(cubeDescMeasureFunction);
				cubeDescMeasures.add(cubeDescMeasure);

				n++;
			}
		}
	}

	private void loadCountMeasure() {
		BuildCubeDescMeasure cubeDescMeasure = new BuildCubeDescMeasure();
		cubeDescMeasure.setName("_COUNT_");
		cubeDescMeasure.setID(1);

		BuildCubeDescMeasureFunction cubeDescMeasureFunction = new BuildCubeDescMeasureFunction();
		cubeDescMeasureFunction.setExpression("COUNT");
		cubeDescMeasureFunction.setReturnType("bigint");
		cubeDescMeasureFunction.setParameters("constant", "1");

		cubeDescMeasure.setFunction(cubeDescMeasureFunction);
		cubeDescMeasures.add(cubeDescMeasure);
	}

	public JSONArray toJSON() {
		JSONArray objs = new JSONArray();

		for (BuildCubeDescMeasure cubeDescMeasure : cubeDescMeasures) {
			objs.put(cubeDescMeasure.toJSON());
		}

		return objs;
	}
}

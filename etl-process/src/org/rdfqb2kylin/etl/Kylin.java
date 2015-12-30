package org.rdfqb2kylin.etl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.rdfqb2kylin.kylin.api.KylinRequest;
import org.rdfqb2kylin.kylin.api.model.BuildCube;
import org.rdfqb2kylin.kylin.api.model.CreateKylinCube;
import org.rdfqb2kylin.kylin.api.model.CreateKylinProject;
import org.rdfqb2kylin.kylin.api.model.SyncHiveTables;
import org.rdfqb2kylin.mdm.Cube;

/*
 * Kylin Class
 * Creates JSON requests
 * - Create new Kylin projects
 * - Sync Hive Tables to Project
 * - Define Cube Model
 * - Build OLAP Cube
 */
public class Kylin {
	private Cube cube;
	private String projectName;
	private String cubeModelName;

	final static Logger logger = Logger.getLogger(Kylin.class);

	public Kylin(Cube cube) {
		this.cube = cube;

		this.projectName = "ssb" + dateID();
		this.cubeModelName = projectName + "_cube";
	}

	// Create new Kylin Project with unique Project Name
	public void createNewProject() {
		CreateKylinProject createNewProjectObject = new CreateKylinProject(this);

		logger.info("Create Kylin project '" + projectName + "'...");
		JSONObject jsonResponse = KylinRequest.post(Property.get("kylin.url") + "/api/projects", createNewProjectObject.toJSON());
		logger.debug(jsonResponse);
	}

	// Sync Star Schema Hive Tables to Kylin
	public void syncHiveTables() {
		SyncHiveTables syncHiveTables = new SyncHiveTables(this);

		logger.info("Sync Hive Tables '" + syncHiveTables + "' to Kylin...");
		JSONObject jsonResponse = KylinRequest.post(Property.get("kylin.url") + "/api/tables/" + syncHiveTables + "/" + projectName);
		logger.debug(jsonResponse);
	}

	// Generate and send Cube Model to Kylin
	public void generateCubeModel() {
		BuildCube buildCube = new BuildCube(this);

		logger.info("Create Cube Model '" + cubeModelName + "' in Kylin...");
		logger.debug("JSON to create Cube Model: " + buildCube.toJSON());
		JSONObject jsonResponse = KylinRequest.post(Property.get("kylin.url") + "/api/cubes", buildCube.toJSON());
		logger.debug(jsonResponse);
	}

	// JSON Request to build OLAP Cube
	public void createCube() {
		CreateKylinCube createCube = new CreateKylinCube();

		logger.debug(createCube.toJSON());
		logger.info("Build Cube '" + cubeModelName + "'...");
		logger.info("Info: This take a while... ... ... ... ...");
		JSONObject jsonResponse = KylinRequest.put(Property.get("kylin.url") + "/api/cubes/" + cubeModelName + "/rebuild", createCube.toJSON());
		logger.debug(jsonResponse);

		try {
			waitUntilFinished(jsonResponse.getString("uuid"));
		} catch (JSONException e) {
			logger.error("Could not load UUID from JSON Object!");
			e.printStackTrace();
		}
	}

	// Print current state and progess to logger while building OLAP Cube in Kylin
	private void waitUntilFinished(String uuid) {
		try {
			boolean finished = false;

			while (!finished) {
				Thread.sleep(10000);

				JSONObject jsonJobResponse = KylinRequest.get(Property.get("kylin.url") + "/api/jobs/" + uuid);
				logger.debug(jsonJobResponse);

				String status = jsonJobResponse.getString("job_status");
				double progress = Math.round(jsonJobResponse.getDouble("progress") * 100) / 100.0d;

				if (status.equals("FINISHED")) {
					finished = true;
				} else if (status.equals("PENDING") || status.equals("RUNNING")) {
					logger.info("Job UUID: '" + uuid + "' | Status: " + status + " | Progress: " + progress);
				} else {
					logger.error("Job UUID: '" + uuid + "' | Status: " + status);
					throw new Exception("Cube Build failed!");
				}
			}

			logger.info("Cube '" + cubeModelName + "' successfully built!");
			logger.info("Have fun with your OLAP Cube ;) ");
		} catch(Exception e) {
			logger.warn("Could not load current state of cube creation!");
			e.printStackTrace();
		}
	}

	// Method for unique Kylin Project Name
	private String dateID() {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
		Date date = new Date();

		return dateFormat.format(date).toString();
	}

	// Mapping between Hive and Kylin datatypes
	public static String getMeasureReturnType(String hiveType) {
		HashMap<String, String> mapper = new HashMap<String, String>();
		mapper.put("int", "bigint");
		mapper.put("double", "decimal");

		return mapper.get(hiveType.toLowerCase());
	}

	public Cube getCube() {
		return cube;
	}

	public String getProjectName() {
		return projectName;
	}

	public String getCubeModelName() {
		return cubeModelName;
	}
}

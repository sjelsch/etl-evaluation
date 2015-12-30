package org.rdfqb2kylin.etl;

import java.sql.Connection;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.benchmark.KylinBenchmark;
import org.rdfqb2kylin.benchmark.MondrianBenchmark;
import org.rdfqb2kylin.connections.HiveConnection;
import org.rdfqb2kylin.mdm.Cube;

public class ETLProcess {
	private final static Logger logger = Logger.getLogger(ETLProcess.class);

	public static void main(String[] args) {
		Connection connection = HiveConnection.getInstance();

		try {

			String kylinProject = Property.get("global.kylinProject");

			if (Property.get("global.runETLProcess").equals("true")) {
				long globalStartTS = System.currentTimeMillis();

				HiveTableGenerator hiveTableGenerator = new HiveTableGenerator(connection);

				// Component 1: Create QB Triples Table
				if(Property.get("global.createHiveQBTriplesTable").equals("true")) {
					long hiveTriplesTableTS = System.currentTimeMillis();

					hiveTableGenerator.createTriplesTable();

					logger.info("Execution Time Component 1: " + (System.currentTimeMillis() - hiveTriplesTableTS));
				}

				// Component 2: Load MDM Model
				if(Property.get("global.loadMDM").equals("true")) {
					logger.info("Component 2: Load MDM Models form Hive Table '" + Property.getQBTriplesName() + "'...");

					long loadMDMTS = System.currentTimeMillis();

					Cube cube = new Cube(connection);
					cube.load();

					logger.info("Execution Time Component 2: " + (System.currentTimeMillis() - loadMDMTS));
					logger.info("Component 2 - Result MDM: " + cube);

					// Component 3: Create Star Schema
					if(Property.get("global.createStarSchema").equals("true")) {
						logger.info("Component 3: Create Star Schema from Hive Table '" + Property.getQBTriplesName() + "'...");

						long createStarSchemaTS = System.currentTimeMillis();

						hiveTableGenerator.setCube(cube);

						if(Property.get("global.createStarSchemaDimensionTables").equals("true")) {
							hiveTableGenerator.createDimensionTables();
						}

						if(Property.get("global.createStarSchemaFactTables").equals("true")) {
							hiveTableGenerator.createFactTable();
						}

						logger.info("Execution Time Component 3: " + (System.currentTimeMillis() - createStarSchemaTS));
						logger.info("Component 3: Star Schema created!");
					}

					// Component 4: Create Kylin Cube
					if(Property.get("global.createKylinCube").equals("true")) {
						logger.info("Component 4: Create Kylin Cube...");

						long createKylinCubeTS = System.currentTimeMillis();

						Kylin kylin = new Kylin(cube);
						kylin.createNewProject();
						kylin.syncHiveTables();
						kylin.generateCubeModel();
						kylin.createCube();
						kylinProject = kylin.getProjectName();

						logger.info("Execution Time Component 4: " + (System.currentTimeMillis() - createKylinCubeTS));
						logger.info("Component 4: Kylin Cube created!");
					}

					// Component 5: Create Mondrian Schema
					if(Property.get("global.createMondrianSchema").equals("true")) {
						logger.info("Component 5: Create Mondrian Schema...");

						long createMondrianSchemeTS = System.currentTimeMillis();

						MondrianSchema mondrian = new MondrianSchema(cube, kylinProject);
						mondrian.createSchema();

						logger.info("Execution Time Component 5: " + (System.currentTimeMillis() - createMondrianSchemeTS));
						logger.info("Component 5: Mondrian Schema created!");
					}
				}

				logger.info("Execution Time Global: " + (System.currentTimeMillis() - globalStartTS));
			}

			// Run SQL Benchmark
			if (Property.get("benchmark.runSQLQueries").equals("true")) {
				logger.info("Running SQL Queries...");
				KylinBenchmark kb = new KylinBenchmark(kylinProject);
				kb.run();
			}

			// Run MDX Benchmark
			if (Property.get("benchmark.runMDXQueries").equals("true")) {
				logger.info("Running MDX Queries...");
				MondrianBenchmark mb = new MondrianBenchmark(Property.get("mondrian.schema_file"), kylinProject);
				mb.run();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

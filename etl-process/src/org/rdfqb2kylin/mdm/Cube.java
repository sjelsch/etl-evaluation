package org.rdfqb2kylin.mdm;

import java.sql.Connection;
import java.util.Collection;

import org.rdfqb2kylin.mdm.loader.DimensionsLoader;
import org.rdfqb2kylin.mdm.loader.FactGenerator;
import org.rdfqb2kylin.mdm.loader.MeasuresLoader;

/*
 * Cube Class
 * - Contains all measure and dimension models
 * - Contains fact model
 */
public class Cube {
	private Connection connection;

	private Collection<Measure> measures;
	private Collection<Dimension> dimensions;
	private Fact fact;

	public Cube(Connection connection) {
		this.connection = connection;
	}

	public void load() {
		MeasuresLoader measuresLoader = new MeasuresLoader(connection);
		DimensionsLoader dimensionsLoader = new DimensionsLoader(connection);

		this.measures = measuresLoader.get();
		this.dimensions = dimensionsLoader.get();
		this.fact = new FactGenerator(measuresLoader.get(), dimensionsLoader.get()).get();
	}

	public Collection<Measure> getMeasures() {
		return measures;
	}

	public Collection<Dimension> getDimensions() {
		return dimensions;
	}

	public Fact getFact() {
		return fact;
	}

	public String toString() {
		StringBuilder cubeSB = new StringBuilder();

		cubeSB.append("\n").append("Measures:");
		for (Measure measure : measures) {
			cubeSB.append(measure);
		}

		cubeSB.append("\n").append("Dimensions:");
		for (Dimension dimension : dimensions) {
			cubeSB.append(dimension);
		}

		cubeSB.append(fact);

		return cubeSB.toString();
	}
}

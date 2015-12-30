package org.rdfqb2kylin.mdm.loader;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.rdfqb2kylin.mdm.Dimension;
import org.rdfqb2kylin.mdm.Fact;
import org.rdfqb2kylin.mdm.Measure;

/*
 * MDM Loader - Fact Generator Class
 * Create links to measures and dimensions.
 */
public class FactGenerator {
	private Fact fact;

	private Collection<Measure> measures;
	private Collection<Dimension> dimensions;

	private final static Logger logger = Logger.getLogger(FactGenerator.class);

	public FactGenerator(Collection<Measure> measures, Collection<Dimension> dimensions) {
		this.fact = new Fact();

		this.measures = measures;
		this.dimensions = dimensions;

		generate();
	}

	private void generate() {
		logger.info("Generating Fact Model...");

		for (Measure measure : measures) {
			logger.debug("Fact Measure Attribute found: " + measure.getUri());
			fact.addMeasure(measure);
		}

		for (Dimension dimension : dimensions) {
			boolean isMeasure = false;
			for (Measure measure : fact.getMeasures()) {
				if (dimension.getUri().equals(measure.getUri())) {
					isMeasure = true;
					break;
				}
			}

			if (isMeasure == false) {
				logger.debug("Fact Foreign Key Attribute found: " + dimension.getUri());
				fact.addDimension(dimension);
			}
		}
	}

	public Fact get() {
		return fact;
	}
}

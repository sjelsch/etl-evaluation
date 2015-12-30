package org.rdfqb2kylin.kylin.api.model;

import org.rdfqb2kylin.etl.Kylin;
import org.rdfqb2kylin.mdm.Dimension;

public class SyncHiveTables {
	private Kylin kylin;

	public SyncHiveTables(Kylin kylin) {
		this.kylin = kylin;
	}

	// Get all Star Schema Dimension Hive Tables and Sync them to Kylin
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Dimension dimension : kylin.getCube().getFact().getDimensions()) {
			sb.append(dimension.getTableName()).append(",");
		}
		sb.append(kylin.getCube().getFact().getTableName());

		return sb.toString();
	}
}

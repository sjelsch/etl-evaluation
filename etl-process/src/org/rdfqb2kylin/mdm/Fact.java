package org.rdfqb2kylin.mdm;

import java.util.ArrayList;

import org.rdfqb2kylin.etl.Property;

/*
 * Fact Class
 * - Includes tableName and column Names
 * - Contains links to measure and dimension models
 */
public class Fact {
	private String tableName;
	private ArrayList<String> columnNameUris;

	private ArrayList<Measure> measures;
	private ArrayList<Dimension> dimensions;

	public Fact() {
		this.columnNameUris = new ArrayList<String>();

		this.tableName = Property.get("hive.fact_table_name").toUpperCase();
		this.measures = new ArrayList<Measure>();
		this.dimensions = new ArrayList<Dimension>();
	}

	public void setTableName(String tableName) {
		this.tableName = tableName.toUpperCase();
	}

	public void addColumnNameUri(String columnName) {
		columnNameUris.add(columnName);
	}

	public void addMeasure(Measure measure) {
		measures.add(measure);
	}

	public void addDimension(Dimension dimension) {
		dimensions.add(dimension);
	}

	public String getTableName() {
		return tableName;
	}

	public ArrayList<String> getColumnNameUris() {
		return columnNameUris;
	}

	public ArrayList<Measure> getMeasures() {
		return measures;
	}

	public ArrayList<Dimension> getDimensions() {
		return dimensions;
	}

	public String toString() {
		StringBuilder fact = new StringBuilder();

		fact.append("Fact:");
		fact.append("\n\t").append("tableName: " + getTableName());

		fact.append("\n\t").append("MeasureUris:");
		for (Measure measure : getMeasures()) {
			fact.append("\n\t\t").append("MeasureUri: " + measure.getUri());
		}

		fact.append("\n\t").append("DimensionUris:");
		for (Dimension dimension : getDimensions()) {
			fact.append("\n\t\t").append("DimensionUri: " + dimension.getUri());
		}

		fact.append("\n\t").append("ColumnNamesUris:");
		for (String columnName : getColumnNameUris()) {
			fact.append("\n\t\t").append("ColumnNameUri: " + columnName);
		}

		return fact.toString();
	}
}

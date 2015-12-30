package org.rdfqb2kylin.mdm;

import org.rdfqb2kylin.util.UriUtil;

/*
 * Measures Class
 * Contains all measure information:
 * - URI
 * - Column Name
 * - Type (range)
 * - aggregator
 *
 * Also it contains calculated member information.
 */
public class Measure extends Model {
	private String componentUri;
	private String columnName;
	private String type;
	private String aggregator;

	private boolean isCalculatedMember;
	private Measure[] calculatedMemberFormula;
	private String mathSign;

	public Measure() {
		this.aggregator = UriUtil.getStandardAggregator();
		this.type = "int";
		this.isCalculatedMember = false;
	}

	public void setComponentUri(String componentUri) {
		this.componentUri = componentUri;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setAggregator(String aggregator) {
		this.aggregator = aggregator;
	}

	public void setIsCalculatedMember(boolean isCalculatedMember) {
		this.isCalculatedMember = isCalculatedMember;
	}

	public void setCalculatedMemberComponents(Measure measure1, Measure measure2) {
		this.calculatedMemberFormula = new Measure[2];
		this.calculatedMemberFormula[0] = measure1;
		this.calculatedMemberFormula[1] = measure2;
	}

	public void setMathSign(String mathSign) {
		this.mathSign = mathSign;
	}

	public String getComponentUri() {
		return componentUri;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getType() {
		return type;
	}

	public String getAggregator() {
		return aggregator;
	}

	public boolean isCalculatedMember() {
		return isCalculatedMember;
	}

	public Measure[] getCalculatedMemberFormula() {
		return calculatedMemberFormula;
	}

	public String getMathSign() {
		return mathSign;
	}

	public boolean hasToBeCalculated() {
		return isCalculatedMember() && (getMathSign().equals("*") || getMathSign().equals("/"));
	}

	public String toString() {
		StringBuilder measure = new StringBuilder();

		measure.append("\n\t").append("Measure: " + getUri());
		measure.append("\n\t\t").append("Name: " + getName());
		measure.append("\n\t\t").append("ColumnName: " + getColumnName());
		measure.append("\n\t\t").append("Type: " + getType());
		measure.append("\n\t\t").append("Aggregate: " + getAggregator());
		measure.append("\n\t\t").append("isCalculatedMember: " + isCalculatedMember());

		if (isCalculatedMember()) {
			measure.append("\n\t\t").append("calculcatedMemberFormula: " + getCalculatedMemberFormula()[0].getUri() + " " + getMathSign() + " " + getCalculatedMemberFormula()[1].getUri());
		}

		return measure.toString();
	}
}

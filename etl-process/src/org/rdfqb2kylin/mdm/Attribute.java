package org.rdfqb2kylin.mdm;

/*
 * Attribute Class
 * Contains all attributes information from dimension instances
 */
public class Attribute extends Model {
	private String columnName;
	private String type;

	private Level level;

	public Attribute() {

	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setLevel(Level level) {
		this.level = level;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getType() {
		return type;
	}

	public Level getLevel() {
		return level;
	}

	public String toString() {
		StringBuilder attribute = new StringBuilder();

		attribute.append("\n\t\t").append("Attribute: " + getUri());
		attribute.append("\n\t\t\t").append("Name: " + getName());
		attribute.append("\n\t\t\t").append("columnName: " + getColumnName());
		attribute.append("\n\t\t\t").append("type: " + getType());

		attribute.append("\n\t\t\t").append("inLevelUris:");
		attribute.append("\n\t\t\t\t").append(level.getUri());

		return attribute.toString();
	}
}

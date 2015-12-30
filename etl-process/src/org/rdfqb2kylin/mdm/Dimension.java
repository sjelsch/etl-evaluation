package org.rdfqb2kylin.mdm;

import java.util.ArrayList;

/*
 * Dimension Class
 * Contains all dimension information
 * - URI
 * - Name
 * - Table Name
 * - Primary Column Name
 *
 * Includes all attribute and hierarchy models.
 */
public class Dimension extends Model {
	private String tableName;
	private String primaryColumnName;

	private ArrayList<Attribute> attributes;
	private ArrayList<Hierarchy> hierarchies;

	public Dimension() {
		this.primaryColumnName = "KEY";

		this.attributes = new ArrayList<Attribute>();
		this.hierarchies = new ArrayList<Hierarchy>();
	}

	public boolean hasAttributes() {
		return (attributes.size() > 0);
	}

	public boolean hasHierarchy() {
		return (hierarchies.size() > 0);
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setPrimaryColumnName(String primaryColumnName) {
		this.primaryColumnName = primaryColumnName;
	}

	public void addAttribute(Attribute attribute) {
		boolean attributeExists = false;

		for (Attribute attributeModel : attributes) {
			if (attribute.getUri().equals(attributeModel.getUri())) {
				attributeExists = true;
				break;
			}
		}

		if (!attributeExists) {
			attributes.add(attribute);
		}
	}

	public void addHierarchy(Hierarchy hierarchy) {
		if (!hierarchies.contains(hierarchy)) {
			hierarchies.add(hierarchy);
		}
	}

	public String getTableName() {
		return tableName;
	}

	public String getPrimaryColumnName() {
		return primaryColumnName;
	}

	public ArrayList<Attribute> getAttributes() {
		return attributes;
	}

	public ArrayList<Hierarchy> getHierarchies() {
		return hierarchies;
	}

	public Hierarchy getLastHierarchy() {
		return getHierarchies().get(getHierarchies().size() - 1);
	}

	public String toString() {
		StringBuilder dimension = new StringBuilder();

		dimension.append("\n\t").append("Dimension: " + getUri());
		dimension.append("\n\t\t").append("Name: " + getName());
		dimension.append("\n\t\t").append("TableName: " + getTableName());
		dimension.append("\n\t\t").append("PrimaryColumnName: " + getPrimaryColumnName());

		dimension.append("\n\t\t").append("Hierarchies:");
		for (Hierarchy hierarchy : getHierarchies()) {
			dimension.append(hierarchy);
		}

		dimension.append("\n\t\t").append("Attributes:");
		for (Attribute attribute : getAttributes()) {
			dimension.append("\n\t\t\t").append(attribute.getUri());
		}

		dimension.append("\n");

		return dimension.toString();
	}
}

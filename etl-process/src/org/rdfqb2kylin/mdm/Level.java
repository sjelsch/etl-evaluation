package org.rdfqb2kylin.mdm;

/*
 * Level Class
 * Contains all level information.
 * Has a parent hierarchy and if present, a parent level model
 */
public class Level extends Model {
	private String parentLevelUri;
	private Level parentLevel;

	private Hierarchy hierarchy;

	private int depth;

	public Level() {
		this.parentLevelUri = null;
	}

	public void setParentLevelUri(String parentLevel) {
		this.parentLevelUri = parentLevel;
	}

	public void setParentLevel(Level parentLevel) {
		this.parentLevel = parentLevel;
	}

	public void setDepth(int depth) {
		this.depth = depth;
	}

	public void setHierarchy(Hierarchy hierarchy) {
		this.hierarchy = hierarchy;
	}

	public String getParentLevelUri() {
		return parentLevelUri;
	}

	public Level getParentLevel() {
		return parentLevel;
	}

	public int getDepth() {
		return depth;
	}

	public Hierarchy getHierarchy() {
		return hierarchy;
	}

	public boolean hasParentLevelUri() {
		if (parentLevelUri != null) {
			return true;
		}
		return false;
	}

	public String toString() {
		StringBuilder level = new StringBuilder();

		level.append("\n\t\t\t").append("Level: " + getUri());
		level.append("\n\t\t\t\t").append("Name: " + getName());
		if (getParentLevel() == null) {
			level.append("\n\t\t\t\t").append("ParentLevel: -");
		} else {
			level.append("\n\t\t\t\t").append("ParentLevel: " + getParentLevel().getUri());
		}
		level.append("\n\t\t\t\t").append("Depth: " + getDepth());

		return level.toString();
	}
}

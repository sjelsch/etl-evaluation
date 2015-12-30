package org.rdfqb2kylin.mdm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/*
 * Hierarchy Class
 * Contains all level information
 * Has a parent Dimension model
 */
public class Hierarchy extends Model {
	private Dimension dimension;

	private ArrayList<Level> levels;

	public Hierarchy() {
		levels = new ArrayList<Level>();
	}

	public void setDimension(Dimension dimension) {
		this.dimension = dimension;
	}

	public void addLevel(Level level) {
		levels.add(level);
	}

	public Dimension getDimension() {
		return dimension;
	}

	public ArrayList<Level> getLevels() {
		Collections.sort(levels, new Comparator<Level>() {
			public int compare(Level level1, Level level2) {
				return level1.getDepth() - level2.getDepth();
			}
		});

		return levels;
	}

	public Level getDeepestLevel() {
		return getLevels().get(getLevels().size() - 1);
	}

	public String toString() {
		StringBuilder hierarchy = new StringBuilder();

		hierarchy.append("\n\t\t").append("Hierarchy: " + getUri());
		hierarchy.append("\n\t\t\t").append("Name: " + getName());

		hierarchy.append("\n\t\t\t").append("Levels:");
		for (Level level : getLevels()) {
			hierarchy.append(level);
		}

		return hierarchy.toString();
	}
}

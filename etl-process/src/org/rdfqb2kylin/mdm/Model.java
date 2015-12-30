package org.rdfqb2kylin.mdm;

/*
 * Model Class
 * Parent Class of Attribute, Dimension, Hierarchy, Level and Measure class.
 * Every Model is described by URI and contains a name.
 */
public class Model {
	private String uri;
	private String name;

	public void setUri(String uri) {
		this.uri = uri;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUri() {
		return uri;
	}

	public String getName() {
		return name;
	}
}

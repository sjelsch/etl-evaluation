package org.rdfqb2kylin.etl;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.rdfqb2kylin.mdm.Attribute;
import org.rdfqb2kylin.mdm.Cube;
import org.rdfqb2kylin.mdm.Dimension;
import org.rdfqb2kylin.mdm.Hierarchy;
import org.rdfqb2kylin.mdm.Level;
import org.rdfqb2kylin.mdm.Measure;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.apache.log4j.Logger;

/*
 * Mondrian Schema Class
 * Creates Mondrian Schema XML-File
 */
public class MondrianSchema {
	private Cube cube;
	private String kylinProjectName;

	private static String FILEPATH = Property.get("mondrian.schema_file");

	private Document doc;

	final static Logger logger = Logger.getLogger(MondrianSchema.class);

	public MondrianSchema(Cube cube, String kylinProjectName) throws ParserConfigurationException {
		this.cube = cube;
		this.kylinProjectName = kylinProjectName;

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		this.doc = docBuilder.newDocument();
	}

	public void createSchema() throws TransformerException {
		// Schema element
		Element schemaElement = doc.createElement("Schema");
		doc.appendChild(schemaElement);
		schemaElement.setAttribute("metamodelVersion", "4.0");
		schemaElement.setAttribute("name", kylinProjectName + " Schema");

		// PhysicalSchema element
		Element physicalSchemaElement = doc.createElement("PhysicalSchema");
		schemaElement.appendChild(physicalSchemaElement);

		// Fact table element
		Element factTableElement = doc.createElement("Table");
		factTableElement.setAttribute("name", cube.getFact().getTableName());
		physicalSchemaElement.appendChild(factTableElement);

		// Dimension table elements
		for (Dimension dimension : cube.getFact().getDimensions()) {
			Element dimensionTableElement = doc.createElement("Table");
			dimensionTableElement.setAttribute("name", dimension.getTableName());

			Element keyElement = doc.createElement("Key");
			Element columnElement = doc.createElement("Column");
			columnElement.setAttribute("name", dimension.getPrimaryColumnName());

			keyElement.appendChild(columnElement);
			dimensionTableElement.appendChild(keyElement);
			physicalSchemaElement.appendChild(dimensionTableElement);
		}

		// Cube element
		Element cubeElement = doc.createElement("Cube");
		cubeElement.setAttribute("name", kylinProjectName);

		// Cube Dimensions
		Element dimensionElements = doc.createElement("Dimensions");
		cubeElement.appendChild(dimensionElements);

		for (Dimension dimension : cube.getDimensions()) {
			if(dimension.hasHierarchy()) {
				// Dimension element
				Element dimensionElement = doc.createElement("Dimension");
				dimensionElement.setAttribute("key", dimension.getPrimaryColumnName());
				dimensionElement.setAttribute("name", dimension.getTableName());
				dimensionElement.setAttribute("table", dimension.getTableName());

				// Dimension Attribute elements
				Element dimensionAttributeElements = doc.createElement("Attributes");

				// Add primary attribute
				Element PrimaryAttributeElement = doc.createElement("Attribute");
				PrimaryAttributeElement.setAttribute("keyColumn", "KEY");
				PrimaryAttributeElement.setAttribute("name", "KEY");

				dimensionAttributeElements.appendChild(PrimaryAttributeElement);

				for (Attribute attribute : dimension.getAttributes()) {
					Element attributeElement = doc.createElement("Attribute");
					attributeElement.setAttribute("keyColumn", attribute.getColumnName());
					attributeElement.setAttribute("name", attribute.getColumnName());

					dimensionAttributeElements.appendChild(attributeElement);
				}

				// Dimension Hierarchy elements
				Element dimensionHierarchyElements = doc.createElement("Hierarchies");

				for (Hierarchy hierarchy : dimension.getHierarchies()) {
					// Hierarchies
					Element dimensionHierarchyElement = doc.createElement("Hierarchy");
					dimensionHierarchyElement.setAttribute("allMemberName", "All " + hierarchy.getName());
					dimensionHierarchyElement.setAttribute("name", hierarchy.getName());

					// Levels
					for (Level level : hierarchy.getLevels()) {
						if (level == hierarchy.getDeepestLevel()) {
							continue;
						}

						Element attributeElement = doc.createElement("Attribute");
						attributeElement.setAttribute("hasHierarchy", "false");
						attributeElement.setAttribute("keyColumn", level.getName());
						attributeElement.setAttribute("name", level.getName());

						Element levelElement = doc.createElement("Level");
						levelElement.setAttribute("attribute", level.getName());

						dimensionAttributeElements.appendChild(attributeElement);
						dimensionHierarchyElement.appendChild(levelElement);
					}

					dimensionHierarchyElements.appendChild(dimensionHierarchyElement);
				}

				dimensionElement.appendChild(dimensionAttributeElements);
				dimensionElement.appendChild(dimensionHierarchyElements);
				dimensionElements.appendChild(dimensionElement);

			// Dimension from Fact Table
			} else if(!dimension.hasAttributes() && !dimension.hasHierarchy()) {
				Element dimensionElement = doc.createElement("Dimension");
				dimensionElement.setAttribute("key", dimension.getTableName());
				dimensionElement.setAttribute("name", dimension.getTableName());

				// Dimension Attribute elements
				Element dimensionAttributeElements = doc.createElement("Attributes");
				Element attributeElement = doc.createElement("Attribute");

				attributeElement.setAttribute("hasHierarchy", "false");
				attributeElement.setAttribute("keyColumn", dimension.getTableName());
				attributeElement.setAttribute("name", dimension.getTableName());
				attributeElement.setAttribute("table", cube.getFact().getTableName());
				dimensionAttributeElements.appendChild(attributeElement);

				Element dimensionHierarchyElements = doc.createElement("Hierarchies");
				Element dimensionHierarchyElement = doc.createElement("Hierarchy");

				dimensionHierarchyElement.setAttribute("name", "default");
				dimensionHierarchyElements.appendChild(dimensionHierarchyElement);

				Element levelElement = doc.createElement("Level");
				levelElement.setAttribute("attribute", dimension.getTableName());

				dimensionAttributeElements.appendChild(attributeElement);
				dimensionHierarchyElement.appendChild(levelElement);

				dimensionElement.appendChild(dimensionAttributeElements);
				dimensionElement.appendChild(dimensionHierarchyElements);
				dimensionElements.appendChild(dimensionElement);
			}
		}

		// MeasureGroups
		Element measureGroupElements = doc.createElement("MeasureGroups");

		Element measureGroupElement = doc.createElement("MeasureGroup");
		measureGroupElement.setAttribute("name", "Facts");
		measureGroupElement.setAttribute("table", cube.getFact().getTableName());

		Element measureElements = doc.createElement("Measures");

		for (Measure measure : cube.getMeasures()) {
			if(measure.isCalculatedMember() && !measure.hasToBeCalculated()) {
				// Nothing to do with Calculated Members with + or - math signs
				continue;
			} else {
				Element measureElement = doc.createElement("Measure");
				measureElement.setAttribute("aggregator", measure.getAggregator());
				measureElement.setAttribute("column", measure.getColumnName());
				measureElement.setAttribute("name", measure.getColumnName());

				measureElements.appendChild(measureElement);
			}
		}

		// DimensionLinks
		Element dimensionLinkElements = doc.createElement("DimensionLinks");
		for (Dimension dimension : cube.getDimensions()) {
			if(dimension.hasHierarchy()) {
				Element foreignKeyLinkElement = doc.createElement("ForeignKeyLink");
				foreignKeyLinkElement.setAttribute("dimension", dimension.getTableName());
				foreignKeyLinkElement.setAttribute("foreignKeyColumn", dimension.getTableName());

				dimensionLinkElements.appendChild(foreignKeyLinkElement);

			// Dimension from Fact Table
			} else if(!dimension.hasHierarchy() && !dimension.hasAttributes()) {
				Element foreignKeyLinkElement = doc.createElement("FactLink");
				foreignKeyLinkElement.setAttribute("dimension", dimension.getTableName());

				dimensionLinkElements.appendChild(foreignKeyLinkElement);
			}
		}

		measureGroupElement.appendChild(measureElements);
		measureGroupElement.appendChild(dimensionLinkElements);

		measureGroupElements.appendChild(measureGroupElement);
		cubeElement.appendChild(measureGroupElements);

		schemaElement.appendChild(cubeElement);

		// Write the content to XML file
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(FILEPATH));

		transformer.transform(source, result);

		logger.info("Mondrian schema saved: " + FILEPATH);
	}

	public String getFilePath() {
		return FILEPATH;
	}
}

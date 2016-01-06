## Optimierung von Analytischen Abfragen über Statistical Linked Data durch Horizontale Skalierung

Dies ist die Projektseite der Abschlussarbeit "Optimierung von Analytischen Abfragen über Statistical Linked Data durch Horizontale Skalierung" von Sébastien Jelsch am Institut für Angewandte Informatik und formale Beschreibungssprachen - Karlsruher Institut für Technologie.

### Struktur der Projektseite

| Ordner        | Beschreibung      |
| ------------- |-------------------|
| [ausarbeitung](https://github.com/sjelsch/etl-evaluation/tree/master/ausarbeitung) | Enthält die Latex-Dateien sowie eine PDF-Version der Abschlussarbeit. |
| [etl-process](https://github.com/sjelsch/etl-evaluation/tree/master/etl-process) | Enthält den Java-Quellcode des implementierten ETL-Prozesses mit allen verwendeten Bibliotheken sowie eine ausführbare JAR-Datei. |
| [kylin](https://github.com/sjelsch/etl-evaluation/tree/master/kylin) | Auflistung der verwendeten MDX- und SQL-Abfragen für Apache Kylin. |
| [lib](https://github.com/sjelsch/etl-evaluation/tree/master/lib) | Verwendete Bibliotheken für die Transformation der RDF-Dateien in das N-Triples-Format. |
| [mysql](https://github.com/sjelsch/etl-evaluation/tree/master/mysql) | MySQL Schema, Import-Skript und eine Auflistung der verwendeten SQL-Abfragen für MySQL. |
| [open_virtuoso](https://github.com/sjelsch/etl-evaluation/tree/master/open_virtuoso) | Insert-Befehle für die Generierung der Levels, Import-Skript, Dump-Prozedur und eine Auflistung der verwendeten SPARQL-Abfragen für Open Virtuoso. |
| [results](https://github.com/sjelsch/etl-evaluation/tree/master/results) | Ergebnisse der Evaluation: 1.) Ausführungsdauer des ETL-Prozesses. 2.) Antwortzeiten der OLAP-Abfragen bei verschiedenen Datenmengen sowie Anzahl an DataNodes. |
| [ttl](https://github.com/sjelsch/etl-evaluation/tree/master/ttl) | BIBM-Konfigurationsdatei für die Transformation der TBL-Daten in das Turtle-Format sowie die verwendete DataStructureDefinition in der Turtle-Notation. |

### Datengenerierung mit dem Star Schema Benchmark

#### Generierung der TBL-Daten
Im Rahmen der Evaluation wurden die TBL-Daten mit dem Star Schema Benchmark [dbgen-Programm](https://github.com/electrum/ssb-dbgen) [1] erstellt.

**Einstellungen**
> DATABASE = SQLSERVER;
> MASCHINE = LINUX;

**Befehle**
> ./dbgen -s 1 -T a
> ./dbgen -s 10 -T a
> ./dbgen -s 20 -T a

#### Transformation der TBL-Daten in die Turtle-Notation
Die Generierung der RDF-Daten wurde mit dem Programm [Business Intelligence Benchmark](http://sourceforge.net/projects/bibm/) (BIBM) durchgeführt.

**Einstellung**
> java -Xmx8192M com.openlinksw.util.csv2ttl.Main

**Befehl**
> ./csv2ttl.sh -ext tbl -schema ../ttl/01_schema.json -d ../rdf-data ../ssb-dbgen/data/*

#### Deklaration der Fakten als QB Observations
Damit die Fakten im QB-Vokabular vorliegen, muss der SED-Befehl zum Hinzufügen der Property *qb:Observation* ausgeführt werden.

**Befehl**
> sed 's/a rdfh:lineorder ;/a rdfh:lineorder ; a <http:\/\/purl.org\/linked-data\/cube#Observation>; <http:\/\/purl.org\/linked-data\/cube#dataSet> <http:\/\/lod2.eu\/schemas\/rdfh-inst#ds>;/g' lineorder.ttl > lineorder_qb.ttl

#### Generierung der Levels in der Turtle-Notation
Hierzu werden die zuvor generierten Dateien *customer.ttl*, *dates.ttl*, *part.ttl* und *supplier.ttl* in Open Virtuoso geladen (s. [Bulk-Import-Prozedur](https://github.com/sjelsch/etl-evaluation/tree/master/open_virtuoso/bulk_import.txt)). Die verwendeten SPARQL-Insert-Befehle sind im Ordner [open_virtuoso](https://github.com/sjelsch/etl-evaluation/tree/master/open_virtuoso) aufgelistet.

**Befehle**
> /usr/local/virtuoso-opensource/bin/isql 1111 dba dba < ~/etl-evaluation/open_virtuoso/insert_customer_levels.rq
> /usr/local/virtuoso-opensource/bin/isql 1111 dba dba < ~/etl-evaluation/open_virtuoso/insert_date_levels.rq
> /usr/local/virtuoso-opensource/bin/isql 1111 dba dba < ~/etl-evaluation/open_virtuoso/insert_part_levels.rq
> /usr/local/virtuoso-opensource/bin/isql 1111 dba dba < ~/etl-evaluation/open_virtuoso/insert_supplier_levels.rq
>
> dump_one_graph('http://lod2.eu/schemas/rdfh#ssb1_ttl_levels', './data_', 1000000000);

#### Ausführung des ETL-Prozesses
Vor der Ausfürung der ETL-Prozesses muss die Datei [config.properties](https://github.com/sjelsch/etl-evaluation/blob/master/config.properties) angepasst werden, z.B. sind hier die JDBC-Konfigurationen für die Verbindung zu Apache Hive zu definieren. Ferner werden in dieser Konfigurationsdatei einige Variablen definiert die den ETL-Prozess steuern.

**Befehl**
> sh etl_process.sh

### Beschreibung der OLAP-Abfragen

Die Evaluation ist an die Arbeit "No Size Fits All – Running the Star Schema Benchmark with SPARQL and RDF Aggregate Views" [2] von Kämpgen und Harth angelehnt. Eine genaue Beschreibung der ebenfalls in dieser Evaluation verwendeten OLAP-Abfragen ist in der dazugehörigen [Projektseite](http://people.aifb.kit.edu/bka/ssb-benchmark/#ssb-queries-as-olap-queries) zu finden.

### Literaturverzeichnis
[1] O’Neil, P., O’Neil, E., Chen, X.: Star Schema Benchmark - Revision 3. Tech. rep., UMass/Boston (2009). ([pdf](http://www.cs.umb.edu/~poneil/StarSchemaB.pdf))

[2] Benedikt Kämpgen, Andreas Harth. No Size Fits All – Running the Star Schema Benchmark with SPARQL and RDF Aggregate Views. ESWC 2013, LNCS 7882, Seiten: 290-304, Springer, Heidelberg, Mai, 2013. ([Projektseite](http://people.aifb.kit.edu/bka/ssb-benchmark/))

# GTF parser for Java Dataframes
A GTF Reader and Writer for [Java DataFrames](https://github.com/nRo/DataFrame).

The GTF Format is implemented according to this documentation:
 
[GFF/GTF File Format](http://www.ensembl.org/info/website/upload/gff.html)

![travis](https://travis-ci.org/nRo/DataFrame-GTF.svg?branch=master)
[![codecov](https://codecov.io/gh/nRo/DataFrame-GTF/branch/master/graph/badge.svg)](https://codecov.io/gh/nRo/DataFrame-GTF)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/44456bac7a024675b07188b46d8d94ed)](https://www.codacy.com/app/nRo/DataFrame-GTF?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=nRo/DataFrame-GTF&amp;utm_campaign=Badge_Grade)

Documentation
-------
[![Javadocs](http://javadoc.io/badge/de.unknownreality/dataframe-gtf.svg?color=blue)](http://javadoc.io/doc/de.unknownreality/dataframe-gtf)

Install
-------

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.unknownreality/dataframe-gtf/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.unknownreality/dataframe-gtf)


Add this to you pom.xml

```xml
<dependencies>
...
    <dependency>
        <groupId>de.unknownreality</groupId>
        <artifactId>dataframe-gtf</artifactId>
        <version>0.2</version>
    </dependency>
...
</dependencies>
```

Build
-----
To build the library from sources:

1) Clone github repository

    $ git clone https://github.com/nRo/DataFrame-GTF.git

2) Change to the created folder and run `mvn install`

    $ cd DataFrame-GTF
    
    $ mvn install

3) Include it by adding the following to your project's `pom.xml`:

```xml
<dependencies>
...
    <dependency>
        <groupId>de.unknownreality</groupId>
        <artifactId>dataframe-gtf</artifactId>
        <version>0.2-SNAPSHOT</version>
    </dependency>
...
</dependencies>
```

Usage
-----
Create a DataFrame from a GTF file
```java
File gtfFile = new File("genome.gtf");
DataFrame df = DataFrame.load(gtfFile,GTFFormat.GTF)
```

Per default, all GTF fields are included in the resulting DataFrame.
Attributes can be added by adding them to the GTF reader.
```java
GTFReader gtfReader = GTFReaderBuilder.create()
                .withAttribute("gene_id")
                .build();
DataFrame df = DataFrame.load(gtfFile, gtfReader);
```
The column type of GTF fields is predefined:

| GTF field | type |
|-----------|---------|
| seqname | String |
| source | String |
| feature | String |
| start | Long |
| end | Long |
| score | Double |
| strand | String |
| frame | Integer |


The type of attributes can be specified
```java
GTFReader gtfReader = GTFReaderBuilder.create()
                .withAttribute("gene_id")
                .withAttribute("test_value", DoubleColumn.class)
                .build();
DataFrame df = DataFrame.load(gtfFile, gtfReader);
```
DataFrames can be written according to the GTF format.

```java
dataFrame.write(new File("result.gtf"), GTFFormat.GTF);
```
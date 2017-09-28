package de.unknownreality.dataframe.gtf;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.StringWriter;

/**
 * Created by Alex on 19.05.2017.
 */
public class GTFTest {

    @Test
    public void readerTest(){

        GTFReader gtfReader = GTFReaderBuilder.create()
                .build();
        DataFrame dataFrame = DataFrame.load(
                "test.gtf",getClass().getClassLoader(),
                gtfReader);


        Assert.assertEquals(4, dataFrame.size());
        Assert.assertEquals(GTFField.values().length, dataFrame.getColumns().size());



        gtfReader = GTFReaderBuilder.create()
                .withGTFField(GTFField.FEATURE)
                .build();
        dataFrame = DataFrame.load(
                "test.gtf",getClass().getClassLoader(),
                gtfReader);

        Assert.assertEquals(4, dataFrame.size());
        Assert.assertEquals(1, dataFrame.getColumns().size());
        Assert.assertEquals("gene", dataFrame.getRow(0).get(GTFField.FEATURE.name));
        Assert.assertEquals("transcript", dataFrame.getRow(1).get(GTFField.FEATURE.name));
        Assert.assertEquals("exon", dataFrame.getRow(2).get(GTFField.FEATURE.name));
        Assert.assertEquals("transcript", dataFrame.getRow(3).get(GTFField.FEATURE.name));


        gtfReader = GTFReaderBuilder.create()
                .withGTFField(GTFField.FEATURE)
                .withAttribute("gene_id")
                .withAttribute("test_value",DoubleColumn.class)
                .build();
        dataFrame = DataFrame.load(
                "test.gtf",getClass().getClassLoader(),
                gtfReader);

        Assert.assertEquals(4, dataFrame.size());
        Assert.assertEquals(3, dataFrame.getColumns().size());
        Assert.assertEquals("gene", dataFrame.getRow(0).get(GTFField.FEATURE.name));
        Assert.assertEquals("ENSG00000223972", dataFrame.getRow(0).get("gene_id"));
        Assert.assertEquals(true, dataFrame.getRow(0).isNA("test_value"));

        Assert.assertEquals("transcript", dataFrame.getRow(1).get(GTFField.FEATURE.name));
        Assert.assertEquals("ENSG00000223972", dataFrame.getRow(1).get("gene_id"));
        Assert.assertEquals((Double)1.3, dataFrame.getRow(1).getDouble("test_value"));


        Assert.assertEquals("exon", dataFrame.getRow(2).get(GTFField.FEATURE.name));
        Assert.assertEquals("ENSG00000223972", dataFrame.getRow(2).get("gene_id"));
        Assert.assertEquals(true, dataFrame.getRow(2).isNA("test_value"));

        Assert.assertEquals("transcript", dataFrame.getRow(3).get(GTFField.FEATURE.name));
        Assert.assertEquals("ENSG00000223972", dataFrame.getRow(3).get("gene_id"));
        Assert.assertEquals((Double)1.2, dataFrame.getRow(3).getDouble("test_value"));
    }

    @Test
    public void writerTest(){
        GTFReader gtfReader = GTFReaderBuilder.create()
                .withAttribute("gene_id")
                .build();
        DataFrame dataFrame = DataFrame.load(
                "test.gtf",getClass().getClassLoader(),
                gtfReader);

        StringWriter stringWriter = new StringWriter();
        dataFrame.write(stringWriter, GTFFormat.GTF);

        String[] lines = stringWriter.toString().split("\n");
        for(String line : lines){
            String[] parts = line.split("\t");
            Assert.assertEquals("1",parts[0]);
            Assert.assertEquals(".",parts[5]);
            String[] attrParts = GTFUtil.splitAttributes(parts[8]);
            Assert.assertEquals("gene_id",attrParts[0]);
            Assert.assertEquals("ENSG00000223972",attrParts[1]);
        }
        Assert.assertEquals(4, lines.length);

    }

    @Test
    public void metaTest(){
        GTFReader gtfReader = GTFReaderBuilder.create()
                .withAttribute("gene_id")
                .withAttribute("test_value", DoubleColumn.class)
                .build();
        DataFrame dataFrame = DataFrame.load(
                "test.gtf",getClass().getClassLoader(),
                gtfReader);

        DataFrameMeta meta = DataFrameMeta.create(dataFrame,
                GTFFormat.GTF.getClass(),GTFFormat.GTF.getWriterBuilder().build());

        Assert.assertEquals("0",meta.getAttributes().get("attr:gene_id"));
        Assert.assertEquals("1",meta.getAttributes().get("attr:test_value"));

        Assert.assertEquals(GTFField.SEQNAME.name,meta.getColumnInformation().get(0).getName());
        Assert.assertEquals(GTFField.SOURCE.name,meta.getColumnInformation().get(1).getName());
        Assert.assertEquals(GTFField.FEATURE.name,meta.getColumnInformation().get(2).getName());
        Assert.assertEquals(GTFField.END.name,meta.getColumnInformation().get(4).getName());
        Assert.assertEquals(GTFField.SCORE.name,meta.getColumnInformation().get(5).getName());
        Assert.assertEquals(GTFField.STRAND.name,meta.getColumnInformation().get(6).getName());
        Assert.assertEquals(GTFField.FRAME.name,meta.getColumnInformation().get(7).getName());
        Assert.assertEquals("gene_id",meta.getColumnInformation().get(8).getName());
        Assert.assertEquals("test_value",meta.getColumnInformation().get(9).getName());

        for(GTFField field : GTFField.values()){
            Assert.assertEquals(field.column.getClass(),meta.getColumns().get(field.name));
        }
        Assert.assertEquals(StringColumn.class,meta.getColumns().get("gene_id"));
        Assert.assertEquals(DoubleColumn.class,meta.getColumns().get("test_value"));

    }
}

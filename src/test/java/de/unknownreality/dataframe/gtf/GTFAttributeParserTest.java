package de.unknownreality.dataframe.gtf;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GTFAttributeParserTest {
    @Test
    public void testGTFAttributeParser(){
        String attr ="gene_id \"ENSG00000223972\"; " +
                "gene_name \"DDX11L1\"; " +
                "gene_source \"havana\"; " +
                "gene_biotype \"transcribed_unprocessed_pseudogene\"; ";

        String[] parts = GTFUtil.splitAttributes(attr);
        assertEquals(8, parts.length);
        assertEquals("gene_id", parts[0]);
        assertEquals("ENSG00000223972", parts[1]);
        assertEquals("gene_name", parts[2]);
        assertEquals("DDX11L1", parts[3]);
        assertEquals("gene_source", parts[4]);
        assertEquals("havana", parts[5]);
        assertEquals("gene_biotype", parts[6]);
        assertEquals("transcribed_unprocessed_pseudogene", parts[7]);

    }
    @Test
    public void testGTFSpecialAttributeParser(){
        String attr ="gene_id \"ENSG00000223972\"; " +
                "gene_name \"DDX11L1;ABC\"; ";

        String[] parts = GTFUtil.splitAttributes(attr);
        assertEquals(4, parts.length);
        assertEquals("gene_id", parts[0]);
        assertEquals("ENSG00000223972", parts[1]);
        assertEquals("gene_name", parts[2]);
        assertEquals("DDX11L1;ABC", parts[3]);

        attr ="gene_id \"ENSG00000223972\"; " +
                "gene_name \"DDX11L1 ABC\"; ";

        parts = GTFUtil.splitAttributes(attr);
        assertEquals(4, parts.length);
        assertEquals("gene_id", parts[0]);
        assertEquals("ENSG00000223972", parts[1]);
        assertEquals("gene_name", parts[2]);
        assertEquals("DDX11L1 ABC", parts[3]);

        attr ="gene_id \"ENSG00000223972\"; " +
                "gene_name \"DDX11L1\\\"ABC\"; ";

        parts = GTFUtil.splitAttributes(attr);
        assertEquals(4, parts.length);
        assertEquals("gene_id", parts[0]);
        assertEquals("ENSG00000223972", parts[1]);
        assertEquals("gene_name", parts[2]);
        assertEquals("DDX11L1\"ABC", parts[3]);

        attr ="gene_id \"ENSG00000223972\"; " +
                "gene_name \"DDX11L1\\\"; ABC\"; ";
        parts = GTFUtil.splitAttributes(attr);
        assertEquals(4, parts.length);
        assertEquals("gene_id", parts[0]);
        assertEquals("ENSG00000223972", parts[1]);
        assertEquals("gene_name", parts[2]);
        assertEquals("DDX11L1\"; ABC", parts[3]);
    }

    @Test
    public void testGFFAttributeParser(){
        String attr ="hid=trf; hstart=1; hend=21";

        String[] parts = GTFUtil.splitAttributes(attr);
        assertEquals(6, parts.length);
        assertEquals("hid", parts[0]);
        assertEquals("trf", parts[1]);
        assertEquals("hstart", parts[2]);
        assertEquals("1", parts[3]);
        assertEquals("hend", parts[4]);
        assertEquals("21", parts[5]);

    }
}

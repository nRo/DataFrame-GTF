package de.unknownreality.dataframe.gtf;

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.LongColumn;
import de.unknownreality.dataframe.column.StringColumn;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
/*
 *
 *  * Copyright (c) 2017 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

/**
 * Created by Alex on 19.05.2017.
 * GTF fields are defined according to this documentation <a href="http://www.ensembl.org/info/website/upload/gff.html">http://www.ensembl.org/info/website/upload/gff.html</a>
 */
public enum GTFField {
    SEQNAME("seqname", 0, new StringColumn("seqname")), SOURCE("source", 1, new StringColumn("source")),
    FEATURE("feature", 2, new StringColumn("feature")), START("start", 3, new LongColumn("start")),
    END("end", 4, new LongColumn("end")), SCORE("score", 5, new DoubleColumn("score")),
    STRAND("strand", 6, new StringColumn("strand")), FRAME("frame", 7, new IntegerColumn("frame"));


    int index;
    String name;
    DataFrameColumn column;


    GTFField(String name, int index, DataFrameColumn column) {
        this.index = index;
        this.name = name;
        this.column = column;
    }

    /**
     * set containing all names of GTF fields
     */
    public static final Set<String> GTF_FIELD_NAMES =  Arrays.stream(GTFField.values())
            .map(GTFField::getName)
            .collect(Collectors.toSet());


    /**
     * returns true if the name matches a GTF field
     * @param name input name
     * @return if name of gtf field
     */
    public static boolean isGTFField(String name){
        return GTF_FIELD_NAMES.contains(name);
    }

    public int getIndex() {
        return index;
    }

    /**
     * name of GTF field
     * @return field name
     */
    public String getName() {
        return name;
    }

    /**
     * Column type of GTF field
     * @return column type
     */
    public Class<? extends DataFrameColumn> getColType() {
        return column.getClass();
    }

    /**
     * Parses an input string to the type specified by the respective column
     * @param value input string
     * @return parsed value
     */
    public Comparable parseValue(String value) {
        try {
            return (Comparable) column.getParser().parse(value);
        } catch (ParseException e) {
            return null;
        }
    }

    public String toString() {
        return name;
    }

    /**
     * Returns the GTF field matching an input string
     * @param str input string
     * @return matching GTF field
     */
    public static GTFField fromString(String str) {
        str = str.toLowerCase();
        for (GTFField c : values()) {
            if (c.name.equals(str)) {
                return c;
            }
        }
        return null;
    }
}

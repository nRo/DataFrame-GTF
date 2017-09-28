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

package de.unknownreality.dataframe.gtf;

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.io.FormatSettings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 17.06.2017.
 */
public class GTFSettings implements FormatSettings {
    private List<GTFField> gtfFields = new ArrayList<>();
    private Map<String, DataFrameColumn> attributes = new LinkedHashMap<>();
    private boolean addAllGTFFields = true;

    public boolean isAddAllGTFFields() {
        return addAllGTFFields;
    }

    public void setAddAllGTFFields(boolean addAllGTFFields) {
        this.addAllGTFFields = addAllGTFFields;
    }

    public List<GTFField> getGtfFields() {
        return gtfFields;
    }

    public Map<String, DataFrameColumn> getAttributes() {
        return attributes;
    }

    /**
     * Adds a GTF field. If no GTF field is specified, all fields will be added to the resulting dataframe
     * @param field gtf field
     */
    public void addGTFField(GTFField field){
        gtfFields.add(field);
        addAllGTFFields = false;
    }

    /**
     * Adds an attribute field with specified column type.
     * @param name attribute name
     * @param column attribute column
     */
    public void addAttribute(String name, DataFrameColumn column){
        attributes.put(name,column);
    }

    /**
     * Adds an attribute field with specified column type.
     * @param name attribute name
     * @param columnClass attribute column type
     */
    public void addAttribute(String name, Class<? extends  DataFrameColumn> columnClass){
        DataFrameColumn column;
        try {
            column = columnClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new DataFrameRuntimeException(
                    String.format("error creating column instance '%s'",columnClass.getCanonicalName()), e);
        }
        addAttribute(name, column);
    }

    /**
     * Adds a <tt>String</tt> attribute field.
     * @param name attribute name
     */
    public void addAttribute(String name){
        addAttribute(name, new StringColumn());
    }


    /**
     * Adds a column to the resulting dataframe. If the column name matches a GTF field, the field is added.
     * Otherwise an attribute is added.
     * @param name column name
x     */
    public void addColumn(String name){
        GTFField field;
        if((field = GTFField.fromString(name)) != null){
            addGTFField(field);
        }
        else{
            addAttribute(name);
        }
    }
}

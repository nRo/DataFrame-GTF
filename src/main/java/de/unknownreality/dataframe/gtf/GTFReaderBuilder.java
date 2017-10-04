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
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.io.ReaderBuilder;

import java.util.Map;

/**
 * Created by Alex on 17.06.2017.
 */
public class GTFReaderBuilder implements ReaderBuilder<GTFRow, GTFReader> {

    public static GTFReaderBuilder create(){
        return new GTFReaderBuilder();
    }

    private GTFSettings settings = new GTFSettings();


    /**
     * Adds a GTF field to the reader. If no GTF field is specified, all fields will be added to the dataframe
     * @param field gtf field
     * @return <tt>self</tt> for method chaining
     */
    public GTFReaderBuilder withGTFField(GTFField field){
        settings.addGTFField(field);
        return this;
    }

    /**
     * Adds an attribute field with specified column type  to the reader.
     * @param name attribute name
     * @param column attribute column
     * @return <tt>self</tt> for method chaining
     */
    public GTFReaderBuilder withAttribute(String name, DataFrameColumn column){
        settings.addAttribute(name, column);
        return this;
    }

    /**
     * Adds an attribute field with specified column type  to the reader.
     * @param name attribute name
     * @param columnClass attribute column type
     * @return <tt>self</tt> for method chaining
     */
    public GTFReaderBuilder withAttribute(String name, Class<? extends  DataFrameColumn> columnClass){
        settings.addAttribute(name, columnClass);
        return this;
    }

    /**
     * Adds a <tt>String</tt> attribute field to the reader.
     * @param name attribute name
     * @return <tt>self</tt> for method chaining
     */
    public GTFReaderBuilder withAttribute(String name){
        settings.addAttribute(name);
        return this;
    }

    /**
     * Adds a column to the reader. If the column name matches a GTF field, the field is added.
     * Otherwise an attribute is added to the reader
     * @param name column name
     * @return <tt>self</tt> for method chaining
     */
    public GTFReaderBuilder withColumn(String name){
        settings.addColumn(name);
        return this;
    }

    public GTFReaderBuilder withPreFilter(String predicate){
        settings.setPreFilter(FilterPredicate.compile(predicate));
        return this;
    }

    public GTFReaderBuilder withPreFilter(FilterPredicate predicate){
        settings.setPreFilter(predicate);
        return this;
    }


    @Override
    public GTFReader build() {
        return new GTFReader(settings);
    }

    @Override
    public ReaderBuilder<GTFRow, GTFReader> loadSettings(Map<String, String> map) throws Exception {
        for(String k : map.keySet()){
            if(k.startsWith("attr:")){
                String colName = k.substring(5);
                withAttribute(colName);
            }
        }
        return this;
    }

    @Override
    public ReaderBuilder<GTFRow, GTFReader> selectColumn(String s) {
        return withColumn(s);
    }


}

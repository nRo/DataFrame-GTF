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

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.io.DataWriter;
import de.unknownreality.dataframe.io.ReadFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by Alex on 17.06.2017.
 */
public class GTFWriter extends DataWriter {

    protected GTFWriter() {
    }


    /**
     * Writes a GTF file based on an input {@link DataContainer}.
     * GTF fields are always added. <tt>null</tt> or <tt>NA</tt> are represented as '.'.
     * All columns not matching GTF fields are added as attributes
     * @param bufferedWriter target writer
     * @param dataContainer input container
     */
    @Override
    public void write(BufferedWriter bufferedWriter, DataContainer<?, ?> dataContainer) {
        try {
            String[] headerNames = new String[dataContainer.getHeader().size()];
            Set<String> headerNameSet = new HashSet<>();
            for(int i = 0; i < headerNames.length; i++){
                headerNames[i] = (String)dataContainer.getHeader().get(i);
                headerNameSet.add(headerNames[i]);
            }
            String fieldsString;
            String attrString;
            for (Row row : dataContainer) {
                StringBuilder gtfFields = new StringBuilder();
                StringBuilder attributes = new StringBuilder();
                for(GTFField gtfField : GTFField.values()){
                    Object v = null;
                    if(headerNameSet.contains(gtfField.name)){
                        v = row.get(gtfField.name);
                    }
                    String s = (v == null || Values.NA.isNA(v)) ? "." : row.getString(gtfField.name);
                    gtfFields.append(s).append('\t');
                }
                for (int i = 0; i < row.size(); i++) {
                    Object v = row.get(i);
                    String s = (v == null || Values.NA.isNA(v)) ? "." : row.getString(i);
                    if(GTFField.isGTFField(headerNames[i])){
                        continue;
                    }
                    else{
                        attributes
                                .append(headerNames[i])
                                .append(" \"")
                                .append(s)
                                .append("\"; ");
                    }
                }
                fieldsString = gtfFields.toString();
                attrString = attributes.toString();
                if(attrString.isEmpty()){
                    fieldsString = fieldsString + ".";
                    bufferedWriter.write(fieldsString);
                }
                else{
                    attrString = attrString.substring(0, attrString.length() - 1);
                    bufferedWriter.write(fieldsString);
                    bufferedWriter.write(attrString);
                }
                bufferedWriter.newLine();
                bufferedWriter.flush();

            }
        } catch (IOException e) {
            throw new DataFrameRuntimeException("error writing gtf", e);
        }
    }

    /**
     * This method is used during the creation of meta files.
     * All settings that should be added to the meta file considering an input dataframe are returned.
     * @param dataFrame input dataframe
     * @return settings written to the meta file
     */
    @Override
    public Map<String, String> getSettings(DataFrame dataFrame) {
        Map<String, String> attributes = new HashMap<>();
        int i = 0;
        for(String columnName : dataFrame.getHeader()){
            if(GTFField.isGTFField(columnName)){
                continue;
            }
            attributes.put("attr:"+columnName,Integer.toString(i++));
        }
        return attributes;
    }

    /**
     * This method is used during the creation of meta files.
     * All columns that should be added to the meta file considering an input dataframe are returned.
     * @param dataFrame input dataframe
     * @return columns written to the meta file
     */
    @Override
    public List<DataFrameColumn> getMetaColumns(DataFrame dataFrame) {
        List<DataFrameColumn> columns = new ArrayList<>();
        for(GTFField gtfField : GTFField.values()){
            columns.add(gtfField.column);
        }
        for(DataFrameColumn column : dataFrame.getColumns()){
            if(!GTFField.isGTFField(column.getName())){
                columns.add(column);
            }
        }
        return columns;
    }


    @Override
    public ReadFormat getReadFormat() {
        return new GTFFormat();
    }
}

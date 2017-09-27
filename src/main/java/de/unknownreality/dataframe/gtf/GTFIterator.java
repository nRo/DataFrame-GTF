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
import de.unknownreality.dataframe.DataFrameException;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.csv.CSVIterator;
import de.unknownreality.dataframe.io.BufferedStreamIterator;
import de.unknownreality.dataframe.io.ColumnInformation;
import de.unknownreality.dataframe.io.DataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.*;

/**
 * Created by Alex on 17.06.2017.
 */
public class GTFIterator extends BufferedStreamIterator<GTFRow> implements DataIterator<GTFRow> {
    private static final Logger log = LoggerFactory.getLogger(CSVIterator.class);
    private static final String[] IGNORE_PREFIXES = new String[]{"#","track","seqname"};

    private int lineNumber = 0;
    private int rowNumber = 0;
    private GTFSettings settings;
    private GTFHeader header = new GTFHeader();

    private List<ColumnInformation> columnInformations = new ArrayList<>();
    private int columnCount;
    private GTFRow bufferedRow = null;
    private Set<Integer> gtfFieldIndices = new HashSet<>();
    private Map<String,Integer> attributeIndexMap = new HashMap<>();

    @SuppressWarnings("unchecked")
    public GTFIterator(BufferedReader reader, GTFSettings settings) {
        super(reader);
        this.settings = settings;
        List<GTFField> gtfFields;
        if(settings.isAddAllGTFFields()){
            gtfFields = Arrays.asList(GTFField.values());
        }
        else{
            gtfFields = settings.getGtfFields();
        }
        columnCount = gtfFields.size() + settings.getAttributes().size();

        Collections.sort(gtfFields, Comparator.comparingInt(o -> o.index));
        int i = 0;
        for(GTFField gtfField : gtfFields){
            gtfFieldIndices.add(gtfField.index);
            header.add(gtfField.name,gtfField.column.getClass(),gtfField.column.getType());
            columnInformations.add(new ColumnInformation(
                 i++,
                 gtfField.name,
                 gtfField.column.getType()
            ));
        }

        for(Map.Entry<String, DataFrameColumn> attribute : settings.getAttributes().entrySet()){
            header.add(attribute.getKey(),
                    attribute.getValue().getClass(),attribute.getValue().getType());
            attributeIndexMap.put(attribute.getKey(), i);
            columnInformations.add(new ColumnInformation(
                    i++,
                    attribute.getKey(),
                    attribute.getValue().getType()
            ));
        }

        loadNext();
    }



    @Override
    public GTFRow next() {
        if(bufferedRow != null){
            GTFRow nextRow = bufferedRow;
            bufferedRow = null;
            return nextRow;
        }
        return super.next();
    }

    /**
     * Reads the gtf input stream and returns a gtf row
     *
     * @return next gtf row
     */
    @Override
    protected GTFRow getNext() {

        try {
            String line = getLine();
            while (line != null && "".equals(line.trim())) {
                line = getLine();
            }
            if (line == null) {
                return null;
            }
            for (String prefix : IGNORE_PREFIXES) {
                if (prefix != null && !"".equals(prefix) && line.startsWith(prefix)) {
                    return getNext();
                }
            }
            String[] values = line.split("\t");
            String[] rowValues = new String[columnCount];
            if(values.length < 9){
                throw new DataFrameRuntimeException(String.format("invalid column count %s < 9",values.length));
            }
            int idx = 0;
            for(int i = 0; i < GTFField.values().length; i++){
                if(settings.isAddAllGTFFields() || gtfFieldIndices.contains(i)){
                    String val = values[i];
                    if(".".equals(val) || "".equals(val)){
                        rowValues[i] = null;
                        continue;
                    }
                    rowValues[idx++] = values[i];
                }
            }
            if(values[8].isEmpty() || values[8].equals(".")){
                return new GTFRow(header, rowValues, rowNumber++);
            }
            String[] attributeParts = GTFUtil.splitAttributes(values[8]);
            if (attributeParts.length %2  != 0) {
                throw new DataFrameException(String.format("error parsing attributes '%s' in line %d", values[8], lineNumber));
            }
            String key;
            Integer attrIdx;
            Set<String> missingAttributes = new HashSet<>(attributeIndexMap.keySet());
            for(int i = 0; i < attributeParts.length; i+= 2){
                key = attributeParts[i];
                if((attrIdx = attributeIndexMap.get(key)) == null){
                    continue;
                }
                rowValues[attrIdx] = attributeParts[i+1];
                missingAttributes.remove(key);
            }
            for(String missingAttribute : missingAttributes){
                attrIdx = attributeIndexMap.get(missingAttribute);
                rowValues[attrIdx] = null;

            }
            return new GTFRow(header, rowValues, rowNumber++);

        } catch (Exception e) {
            log.error("error reading file: {}:{}", lineNumber, e);
            close();
            throw new DataFrameRuntimeException(String.format("error reading gtf row: %d", lineNumber),e);
        }
        finally {
            lineNumber++;
        }
    }

    @Override
    public List<ColumnInformation> getColumnsInformation() {
        return columnInformations;
    }

    @Override
    public Iterator<GTFRow> iterator() {
        return this;
    }
}

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


    public GTFReaderBuilder withGTFField(GTFField field){
        settings.addGTFField(field);
        return this;
    }

    public GTFReaderBuilder withAttribute(String name, DataFrameColumn column){
        settings.addAttribute(name, column);
        return this;
    }

    public GTFReaderBuilder withAttribute(String name, Class<? extends  DataFrameColumn> columnClass){
        settings.addAttribute(name, columnClass);
        return this;
    }

    public GTFReaderBuilder withAttribute(String name){
        settings.addAttribute(name);
        return this;
    }

    public GTFReaderBuilder withColumn(String name){
        settings.addColumn(name);
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

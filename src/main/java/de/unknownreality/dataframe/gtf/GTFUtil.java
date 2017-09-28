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

import java.util.ArrayList;
import java.util.List;

public class GTFUtil {
    /**
     * Splits the attributes part of a row in a GTF file into separate parts.
     * <tt>gene_id "ENSG00000223972"; gene_name "DDX11L1";"</tt> -> <code>["gene_id","ENSG00000223972","gene_name","DDX11L1"]</code>
     * Escaping (<tt>gene_name "xx\"xx";</tt>) and quotations (<tt>gene_name "xx;xx";</tt>) are considered.
     * @param input attributes string
     * @return array containing all attribute parts
     */
    public static String[] splitAttributes(String input) {
        boolean inQuotation = false;
        boolean inDoubleQuotation = false;
        boolean escapeNext = false;
        int currentStart = 0;
        char[] chars = input.trim().toCharArray();
        char c;
        String p;
        boolean startOrSplit = true;
        boolean partQuoted = false;
        boolean containsEscapeChar = false;
        List<String> parts = new ArrayList<>(32);
        for (int i = 0; i < chars.length; i++) {
            c = chars[i];
            boolean escape = escapeNext;
            escapeNext = false;
            if (!escape && c == '\\') {
                chars[i] = Character.MIN_VALUE;
                escapeNext = true;
                containsEscapeChar = true;
            } else if (!escape && c == '\'') {
                if (inQuotation && !escape) {
                    inQuotation = false;
                    partQuoted = true;
                    continue;
                }
                if (!inDoubleQuotation && startOrSplit) {
                    inQuotation = true;
                    currentStart++;
                }
                startOrSplit = false;
            } else if (!escape && c == '\"') {
                if (inDoubleQuotation && !escape) {
                    inDoubleQuotation = false;
                    partQuoted = true;
                    continue;
                }
                if (!inDoubleQuotation && startOrSplit) {
                    inDoubleQuotation = true;
                    currentStart++;
                }
                startOrSplit = false;
            } else if (!escape && (c == ' ' || c == ';' || c == '=') && !inDoubleQuotation && !inQuotation) {
                int length = i - currentStart;
                if(partQuoted){
                    length = length - 1;
                    partQuoted = false;
                }
                if (length == 0) {
                    currentStart++;
                    continue;
                } else {
                    p = new String(chars,currentStart, length);
                    if(containsEscapeChar){
                        p = p.replace(Character.toString(Character.MIN_VALUE),"");
                    }
                }
                parts.add(p);
                currentStart = i + 1;
                startOrSplit = true;
            }
            else{
                startOrSplit = false;
            }
        }
        if (currentStart < chars.length) {
            int length = chars.length - currentStart;
            if(partQuoted){
                length = length - 1;
            }
            p = new String(chars,currentStart, length);
            if(containsEscapeChar){
                p = p.replace(Character.toString(Character.MIN_VALUE),"");
            }
            parts.add(p);
        }
        return parts.toArray(new String[parts.size()]);
    }
}

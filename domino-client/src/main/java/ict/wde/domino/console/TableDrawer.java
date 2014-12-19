/**
 *  Copyright 2014 ZhenZhao and Tieying Zhang. 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package ict.wde.domino.console;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class TableDrawer {

  String[] titles;
  final List<String[]> data = new LinkedList<String[]>();
  int[] colLen;
  int tabLen;

  public TableDrawer(String[] titles) {
    this.titles = titles;
    colLen = new int[titles.length];
    tabLen = 0;
    for (int i = 0; i < titles.length; ++i) {
      colLen[i] = titles[i].length();
      tabLen += colLen[i];
    }
  }

  public void clearData() {
    data.clear();
    tabLen = 0;
    for (int i = 0; i < titles.length; ++i) {
      colLen[i] = titles[i].length();
      tabLen += colLen[i];
    }
  }

  public void addData(String... dat) {
    if (dat == null) return;
    String[] row = new String[titles.length];
    for (int i = 0; i < row.length; ++i) {
      if (i < dat.length && dat[i] != null) {
        row[i] = dat[i];
        if (colLen[i] < dat[i].length()) {
          tabLen = tabLen - colLen[i] + dat[i].length();
          colLen[i] = dat[i].length();
        }
        continue;
      }
      row[i] = "";
    }
    data.add(row);
  }

  public String toString() {
    if (titles.length == 0) return "";
    StringBuffer sb = new StringBuffer(1024);
    Iterator<String[]> it = data.iterator();
    int rowLen = tabLen + 4 + 3 * (titles.length - 1);
    appendChar(sb, '-', rowLen);
    sb.append('\n').append("| ");
    sb.append(titles[0]);
    appendChar(sb, ' ', colLen[0] - titles[0].length());
    for (int i = 1; i < titles.length; ++i) {
      sb.append(" | ").append(titles[i]);
      appendChar(sb, ' ', colLen[i] - titles[i].length());
    }
    sb.append(" |\n");
    appendChar(sb, '-', rowLen);
    while (it.hasNext()) {
      String row[] = it.next();
      sb.append('\n').append("| ");
      sb.append(row[0]);
      appendChar(sb, ' ', colLen[0] - row[0].length());
      for (int i = 1; i < row.length; ++i) {
        sb.append(" | ").append(row[i]);
        appendChar(sb, ' ', colLen[i] - row[i].length());
      }
      sb.append(" |\n");
      appendChar(sb, '-', rowLen);
    }
    return sb.toString();
  }

  static void appendChar(StringBuffer sb, char c, int len) {
    for (int i = 0; i < len; ++i) {
      sb.append(c);
    }
  }

  public static String draw(String[][] data) {
    if (data == null || data.length == 0) return null;
    TableDrawer td = new TableDrawer(data[0]);
    for(int i = 1; i < data.length; ++i) {
      td.addData(data[i]);
    }
    return td.toString();
  }

  public static void main(String[] args) {
    TableDrawer td = new TableDrawer(new String[] { "fddfsa", "aa", "sd-aaa" });
    td.addData("11");
    td.addData("11", "22222222222", "33");
    td.addData("1fdsa1", "2222a", "33");
    System.out.println(td.toString());
  }
}

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

import ict.wde.domino.client.Domino;
import ict.wde.domino.client.Transaction;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

public class CmdCreate extends Command {
  /*
   * CREATE TABLE <table_name> cf1, cf2, cf3, ...;
   */
  static final Pattern ptable = Pattern
      .compile("^[cC][rR][eE][aA][tT][eE] [tT][aA][bB][lL][eE] ");

  HTableDescriptor tableDescriptor;

  public CmdCreate(String sql, Domino domino, Transaction trx) {
    super(sql, domino, trx);
    getTable();
  }

  private void getTable() {
    Matcher mt = ptable.matcher(sql);
    if (!mt.find()) {
      throw new RuntimeException("Table name not found.");
    }
    StringBuffer sb = new StringBuffer(sql.length());
    mt.appendReplacement(sb, "");
    mt.appendTail(sb);
    String table = sb.toString().trim();
    int idx = table.indexOf(' ');
    if (idx == -1) {
      throw new RuntimeException("Column families not found.");
    }
    String cols = table.substring(idx + 1, table.length() - 1).trim();
    if (cols.length() == 0) {
      throw new RuntimeException("Column families not found.");
    }
    table = table.substring(0, idx);
    tableDescriptor = new HTableDescriptor(table.getBytes());
    for (String c : cols.split(",")) {
      HColumnDescriptor cf = new HColumnDescriptor(c.trim().getBytes());
      tableDescriptor.addFamily(cf);
    }
    tableDescriptor
        .addFamily(new HColumnDescriptor(ConsoleConst.DEFAULT_FAMILY));
  }

  @Override
  public String[][] execute() throws IOException {
    domino().createTable(tableDescriptor);
    return null;
  }

  @Override
  public int numRowsInvolved() {
    return 0;
  }

  @Override
  public boolean isTransactionOperation() {
    return false;
  }

}

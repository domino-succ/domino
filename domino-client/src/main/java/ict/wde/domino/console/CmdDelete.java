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

import org.apache.hadoop.hbase.client.HTable;

public class CmdDelete extends Command {
  /*
   * UPSERT <table> values col1=val1, col2=val2, ... where row=key;
   */
  static final Pattern ptable = Pattern
      .compile("[fF][rR][oO][mM][ ].*?[ ][wW][hH][eE][rR][eE]");
  static final Pattern pcondition = Pattern
      .compile("[wW][hH][eE][rR][eE][ ].*?;");

  String table;
  Condition cond;
  byte[] row;

  int numRows = 0;

  public CmdDelete(String sql, Domino domino, Transaction trx) {
    super(sql, domino, trx);
    getTable();
    getCondition();
  }

  private void getTable() {
    Matcher mt = ptable.matcher(sql);
    if (!mt.find()) {
      throw new RuntimeException("No table specified.");
    }
    table = mt.group();
    table = table.substring(5, table.length() - 6).trim();
  }

  private void getCondition() {
    Matcher mt = pcondition.matcher(sql);
    if (!mt.find()) {
      throw new RuntimeException("No rowkey specified.");
    }
    cond = new Condition(mt.group().substring(6));
    if (cond.isScan()) {
      throw new RuntimeException("Invalid rowkey: scan not supported.");
    }
    row = cond.get().getRow();
  }

  @Override
  public String[][] execute() throws IOException {
    HTable table = new HTable(domino().config(), this.table.getBytes());
    this.transaction().delete(row, table);
    table.close();
    numRows = 1;
    return null;
  }

  @Override
  public int numRowsInvolved() {
    return numRows;
  }

  public static void main(String[] args) {
    String sql = "delete from aaa where row=444;";
    CmdDelete cu = new CmdDelete(sql, null, null);
    System.out.println(cu.table);
    System.out.println(new String(cu.row));
  }

  @Override
  public boolean isTransactionOperation() {
    return true;
  }
}

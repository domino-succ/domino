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
import org.apache.hadoop.hbase.client.Put;

public class CmdUpsert extends Command {
  /*
   * UPSERT <table> values col1=val1, col2=val2, ... where row=key;
   */
  static final Pattern ptable = Pattern
      .compile("^[uU][pP][sS][eE][rR][tT][ ].*?[ ][vV][aA][lL][uU][eE][sS]");
  static final Pattern pvalues = Pattern
      .compile("[vV][aA][lL][uU][eE][sS][ ].*?[ ][wW][hH][eE][rR][eE]");
  static final Pattern pcondition = Pattern
      .compile("[wW][hH][eE][rR][eE][ ].*?;");

  String table;
  Condition cond;
  Put put;
  boolean locking;

  int numRows = 0;

  public CmdUpsert(String sql, Domino domino, Transaction trx, boolean locking) {
    super(sql, domino, trx);
    this.locking = locking;
    getTable();
    getCondition();
    getValues();
  }

  private void getValues() {
    Matcher mt = pvalues.matcher(sql);
    if (!mt.find()) {
      throw new RuntimeException("No values specified.");
    }
    String vals = mt.group();
    vals = vals.substring(7, vals.length() - 6).trim();
    String[] val = vals.split(",");
    for (String v : val) {
      int idx = v.indexOf("=");
      if (idx == -1) {
        throw new RuntimeException("Invalid values grammar.");
      }
      String cf = v.substring(0, idx).trim();
      if (cf.length() == 0) {
        throw new RuntimeException("Invalid values grammar.");
      }
      String cfv = v.substring(idx + 1).trim();

      byte[][] cfc = CmdSelect.getCfAndCol(cf);
      put.add(cfc[0], cfc[1], cfv.getBytes());
    }
  }

  private void getTable() {
    Matcher mt = ptable.matcher(sql);
    if (!mt.find()) {
      throw new RuntimeException("No table specified.");
    }
    table = mt.group();
    table = table.substring(7, table.length() - 7).trim();
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
    put = new Put(cond.get().getRow());
  }

  @Override
  public String[][] execute() throws IOException {
    HTable htable = new HTable(domino().config(), table);
    if (!locking) transaction().put(put, htable);
    else transaction().putStateful(put, htable);
    htable.close();
    numRows = 1;
    return null;
  }

  @Override
  public int numRowsInvolved() {
    return numRows;
  }

  public static void main(String[] args) {
    String sql = "upsert abc values a.a=v1, b.b = v2 where row=444;";
    CmdUpsert cu = new CmdUpsert(sql, null, null, false);
    System.out.println(cu.table);
    Put put = cu.put;
    System.out.println(new String(put.getRow()));
    put.get("a".getBytes(), "a".getBytes());
    System.out.println(new String(put.get("b".getBytes(), "b".getBytes())
        .get(0).getValue()));
  }

  @Override
  public boolean isTransactionOperation() {
    return true;
  }
}

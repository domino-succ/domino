/**
 *  Domino, A Transaction Engine Based on Apache HBase
 *  Copyright (C) 2014  Zhen Zhao
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

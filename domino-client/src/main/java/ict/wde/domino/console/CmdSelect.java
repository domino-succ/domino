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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class CmdSelect extends Command {

  static final Pattern pfields = Pattern
      .compile("^[sS][eE][lL][eE][cC][tT][ ].*?[ ][fF][rR][oO][mM]");
  static final Pattern ptable = Pattern
      .compile("[fF][rR][oO][mM][ ].*?[ ][wW][hH][eE][rR][eE]");
  static final Pattern ptable_nocond = Pattern
      .compile("[fF][rR][oO][mM][ ].*?;");
  static final Pattern pcondition = Pattern
      .compile("[wW][hH][eE][rR][eE][ ].*?;");

  String[] fields;
  String table;
  Condition condition;

  int numRows = 0;

  public CmdSelect(String sql, Domino domino, Transaction trx) {
    super(sql, domino, trx);
    getTable();
    getCondition();
    getFields();
  }

  private void getCondition() {
    Matcher mt = pcondition.matcher(sql);
    if (!mt.find()) {
      condition = new Condition(null);
      return;
    }
    condition = new Condition(mt.group().substring(6));
  }

  private void getTable() {
    Matcher mt = ptable.matcher(sql);
    if (!mt.find()) {
      mt = ptable_nocond.matcher(sql);
      if (!mt.find()) {
        throw new RuntimeException("No table specified.");
      }
      table = mt.group().trim();
      table = table.substring(5, table.length() - 1).trim();
      return;
    }
    table = mt.group();
    table = table.substring(5, table.length() - 6).trim();
  }

  private void getFields() {
    Matcher mt = pfields.matcher(sql);
    if (!mt.find()) {
      throw new RuntimeException("Invalid select sql.");
    }
    String field = mt.group().trim();
    field = field.substring(6, field.length() - 4).trim();
    if ("*".equals(field)) {
      fields = null;
      return;
    }
    fields = field.split(",");
    if (fields.length == 0) {
      throw new RuntimeException("Grammar error in fields clause.");
    }
    for (int i = 0; i < fields.length; ++i) {
      fields[i] = fields[i].trim();
      if (fields[i].length() == 0) {
        throw new RuntimeException("Grammar error in fields clause.");
      }
      byte[][] cfc = getCfAndCol(fields[i]);
      if (condition.isScan()) {
        condition.scan().addColumn(cfc[0], cfc[1]);
      }
      else {
        condition.get().addColumn(cfc[0], cfc[1]);
      }
    }
  }

  @Override
  public String[][] execute() throws IOException {
    numRows = 0;
    List<Result> res = new ArrayList<Result>();
    HTable htable = new HTable(domino().config(), table);
    if (condition.isScan()) {
      ResultScanner rs = transaction().scan(condition.scan(), htable);
      for (Result r : rs) {
        if (r.isEmpty()) continue;
        res.add(r);
        ++numRows;
      }
    }
    else {
      Result r = transaction().get(condition.get(), htable);
      if (r != null && !r.isEmpty()) {
        res.add(r);
        numRows = 1;
      }
    }
    htable.close();
    return toDrawableResult(res);
  }

  @Override
  public int numRowsInvolved() {
    return numRows;
  }

  private String[][] toDrawableResult(List<Result> res) {
    String[][] ret = new String[res.size() + 1][];
    if (fields == null) {
      ret[0] = new String[2];
      ret[0][0] = "";
      ret[0][1] = "*";
      for (int i = 0; i < res.size(); ++i) {
        int row = i + 1;
        ret[row] = new String[2];
        Result r = res.get(i);
        ret[row][0] = new String(r.getRow());
        ret[row][1] = resultToString(r);
      }
    }
    else {
      ret[0] = new String[fields.length + 1];
      ret[0][0] = "";
      for (int c = 0; c < fields.length; ++c) {
        ret[0][c + 1] = fields[c];
      }
      for (int i = 0; i < res.size(); ++i) {
        int row = i + 1;
        ret[row] = new String[fields.length + 1];
        Result r = res.get(i);
        ret[row][0] = new String(r.getRow());
        for (int c = 0; c < fields.length; ++c) {
          byte[][] cfc = getCfAndCol(fields[c]);
          byte[] val = r.getValue(cfc[0], cfc[1]);
          ret[row][c + 1] = val == null ? "<null>" : new String(val);
        }
      }
    }
    return ret;
  }

  static String resultToString(Result r) {
    if (r == null || r.isEmpty()) return "<null>";
    StringBuffer sb = new StringBuffer(64);
    Map<byte[], NavigableMap<byte[], byte[]>> map = r.getNoVersionMap();
    for (byte[] cf : map.keySet()) {
      for (byte[] col : map.get(cf).keySet()) {
        sb.append(new String(cf)).append(".").append(new String(col))
            .append("=");
        byte[] val = map.get(cf).get(col);
        sb.append(val == null ? "<null>" : new String(val)).append(", ");
      }
    }
    return sb.toString();
  }

  static byte[][] getCfAndCol(String f) {
    byte[][] ret = new byte[2][];
    int idx = f.indexOf('.');
    if (idx == -1) {
      ret[0] = ConsoleConst.DEFAULT_FAMILY;
      ret[1] = f.getBytes();
    }
    else {
      String cf = f.substring(0, idx);
      String col = f.substring(idx + 1);
      ret[0] = cf.getBytes();
      ret[1] = col.getBytes();
    }
    return ret;
  }

  public static void main(String[] args) throws Exception {
    String sql = "select * from abc where row < 444 and row < 321;";
    CmdSelect cs = new CmdSelect(sql, null, null);
    if (!cs.condition.isScan()) {
      System.out.println("Get: " + new String(cs.condition.get().getRow()));
    }
    else {
      Scan scan = cs.condition.scan();
      System.out.println(String.format("Scan: %s ~ %s",
          scan.getStartRow() == null ? "--" : new String(scan.getStartRow()),
          scan.getStopRow() == null ? "--" : new String(scan.getStopRow())));
    }
  }

  @Override
  public boolean isTransactionOperation() {
    return true;
  }

}

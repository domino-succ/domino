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

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

public class CmdDrop extends Command {

  static final Pattern ptable = Pattern
      .compile("^[dD][rR][oO][pP] [tT][aA][bB][lL][eE] ");

  String table;

  public CmdDrop(String sql, Domino domino, Transaction trx) {
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
    table = sb.toString();
    table = table.substring(0, table.length() - 1).trim();
  }

  @Override
  public String[][] execute() throws IOException {
    domino.dropTable(table);
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

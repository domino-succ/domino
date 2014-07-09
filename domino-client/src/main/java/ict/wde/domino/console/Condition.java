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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

public class Condition {

  private boolean isScan = true;
  private Scan scan = null;
  private Get get = null;

  public Condition(String cond) {
    if (cond == null) {
      scan = new Scan();
      return;
    }
    cond = cond.trim();
    if (cond.length() <= 1) {
      throw new RuntimeException("Invalid condition.");
    }
    if (cond.charAt(cond.length() - 1) == ';') {
      cond = cond.substring(0, cond.length() - 1);
    }
    String conds[] = cond.split("[aA][nN][dD]");
    if (conds.length <= 0 || conds.length > 2) {
      throw new RuntimeException("Invalid condition.");
    }
    if (cond.indexOf('<') == -1 && cond.indexOf('>') == -1) {
      if (conds.length > 1) {
        throw new RuntimeException("Invalid condition.");
      }
      initGet(cond);
      return;
    }
    initScan(conds);
  }

  public Scan scan() {
    return scan;
  }

  public Get get() {
    return get;
  }

  public boolean isScan() {
    return isScan;
  }

  private void initScan(String[] conds) {
    scan = new Scan();
    String start = null;
    String end = null;
    for (int i = 0; i < conds.length; ++i) {
      String cond = conds[i];
      int l = 0;
      while (cond.charAt(l) != '<' && cond.charAt(l) != '>') {
        ++l;
        if (l >= cond.length()) {
          throw new RuntimeException("Invalid condition.");
        }
      }
      if (l >= cond.length() - 1) {
        throw new RuntimeException("Invalid condition.");
      }
      int r = cond.charAt(l + 1) == '=' ? l + 2 : l + 1;
      if (r >= cond.length() - 1) {
        throw new RuntimeException("Invalid condition.");
      }
      String val = cond.substring(r).trim();
      switch (cond.charAt(l)) {
      case '<':
        if (end != null && end.compareTo(val) <= 0) {
          break;
        }
        scan.setStopRow(val.getBytes());
        end = val;
        break;
      case '>':
        if (start != null && start.compareTo(val) >= 0) {
          break;
        }
        scan.setStartRow(val.getBytes());
        start = val;
        break;
      }
    }
  }

  private void initGet(String cond) {
    int idx = cond.indexOf('=');
    if (idx == -1) {
      throw new RuntimeException("Invalid condition.");
    }
    get = new Get(cond.substring(idx + 1).trim().getBytes());
    isScan = false;
  }

}

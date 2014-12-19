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

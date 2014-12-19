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

public abstract class Command {

  protected final Domino domino;
  protected final Transaction trx;
  protected final String sql;

  public Command(String sql, Domino domino, Transaction trx) {
    this.domino = domino;
    this.sql = sql.trim();
    if (!sql.endsWith(";")) sql += ';';
    this.trx = trx;
  }

  public Domino domino() {
    return domino;
  }

  public String sql() {
    return sql;
  }

  public Transaction transaction() {
    return trx;
  }

  public abstract String[][] execute() throws IOException;

  public abstract int numRowsInvolved();

  public abstract boolean isTransactionOperation();

}

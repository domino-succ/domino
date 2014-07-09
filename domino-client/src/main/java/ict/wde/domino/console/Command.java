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

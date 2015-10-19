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

package ict.wde.domino.client;

import ict.wde.domino.common.DominoConst;
import ict.wde.domino.common.DominoIface;
import ict.wde.domino.common.writable.DResult;
import ict.wde.domino.id.DominoIdIface;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Read Only Transaction implementation.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class ReadOnlyTransaction implements Transaction {

  private boolean readyToContinue = true;
  private final HTableInterface metaTable;
  private final Configuration conf;
  private final long startId;
  private final Map<byte[], HTableInterface> tables = new TreeMap<byte[], HTableInterface>(
      Bytes.BYTES_COMPARATOR);

  protected ReadOnlyTransaction(Configuration conf, DominoIdIface tidClient)
      throws IOException {
    this.conf = conf;
    this.metaTable = new HTable(conf, DominoConst.TRANSACTION_META);
    Field f = null;
    try {
      f = metaTable.getClass().getDeclaredField("cleanupConnectionOnClose");
      f.setAccessible(true);
      f.set(metaTable, false);
    } catch (Exception e) {
      e.printStackTrace();
    }
    /*
     * Read-only transactions don't need to create transaction status rows in
     * meta table.
     * 
     * To get a start id is the only additional work to do.
     */
    this.startId = tidClient.getId();
  }

  public Result get(Get get, byte[] table) throws IOException {
    return get(get, getTable(table));
  }

  public Result get(Get get, HTableInterface table) throws IOException {
    checkIfReadyToContinue();
    try {
      DominoIface iface = table.coprocessorProxy(DominoIface.class,
          get.getRow());
      DResult res = iface.get(get, startId);
      if (res.getErrMsg() != null) {
        throw new IOException(res.getErrMsg());
      }
      return res.getResult();
    }
    catch (IOException e) {
      disable();
      throw e;
    }
    catch (Throwable t) {
      disable();
      throw new IOException(t);
    }
  }

  public ResultScanner scan(Scan scan, byte[] table) throws IOException {
    return scan(scan, getTable(table));
  }

  public ResultScanner scan(Scan scan, HTableInterface table)
      throws IOException {
    checkIfReadyToContinue();
    if (scan.hasFamilies()) {
      scan.addFamily(DominoConst.INNER_FAMILY);
    }
    scan.setTimeRange(0, startId + 1);
    scan.setMaxVersions();
    return new DResultScanner(table.getScanner(scan), startId, metaTable,
        table, this);
  }

  private HTableInterface getTable(byte[] name) throws IOException {
    HTableInterface table = tables.get(name);
    if (table == null) {
      table = new HTable(conf, name);
      Field f = null;
      try {
        f = table.getClass().getDeclaredField("cleanupConnectionOnClose");
        f.setAccessible(true);
        f.set(table, false);
      } catch (Exception e) {
        e.printStackTrace();
      }
      tables.put(name, table);
    }
    return table;
  }

  private void closeAllTables() {
    for (Map.Entry<byte[], HTableInterface> ent : tables.entrySet()) {
      try {
        ent.getValue().close();
      }
      catch (IOException ioe) {
      }
    }
    try {
      this.metaTable.close();
    }
    catch (IOException ioe) {
    }
    tables.clear();
  }

  @Override
  public void commit() throws IOException {
    /*
     * Do nothing.
     */
    closeAllTables();
  }

  @Override
  public void rollback() throws IOException {
    /*
     * Do nothing.
     */
    closeAllTables();
  }

  @Override
  public void checkIfReadyToContinue() throws IOException {
    if (!readyToContinue) {
      throw new IOException("Unable to continue. Please restart.");
    }
  }

  @Override
  public void disable() {
    readyToContinue = false;
  }

  @Override
  public long getStartId() {
    return startId;
  }

  @Override
  public void putStateful(Put put, byte[] table) throws IOException {
    throw new UnsupportedOperationException(
        "Could not write data in a read-only transaction");
  }

  @Override
  public void putStateful(Put put, HTableInterface table) throws IOException {
    throw new UnsupportedOperationException(
        "Could not write data in a read-only transaction");
  }

  @Override
  public void put(Put put, byte[] table) throws IOException {
    throw new UnsupportedOperationException(
        "Could not write data in a read-only transaction");
  }

  @Override
  public void put(Put put, HTableInterface table) throws IOException {
    throw new UnsupportedOperationException(
        "Could not write data in a read-only transaction");
  }

  @Override
  public void delete(byte[] row, byte[] table) throws IOException {
    throw new UnsupportedOperationException(
        "Could not write data in a read-only transaction");
  }

  @Override
  public void delete(byte[] row, HTableInterface table) throws IOException {
    throw new UnsupportedOperationException(
        "Could not write data in a read-only transaction");
  }

}

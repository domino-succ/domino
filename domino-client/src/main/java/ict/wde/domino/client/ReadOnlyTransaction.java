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

package ict.wde.domino.client;

import ict.wde.domino.common.DominoConst;
import ict.wde.domino.common.DominoIface;
import ict.wde.domino.common.writable.DResult;
import ict.wde.domino.id.DominoIdIface;

import java.io.IOException;
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

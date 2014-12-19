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
import ict.wde.domino.common.TMetaIface;
import ict.wde.domino.common.writable.DResult;
import ict.wde.domino.id.DominoIdIface;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import jline.internal.Log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Common read&write transaction implementation.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class RWTransaction implements Transaction {

  /**
   * Struct-like class that stores the write history of the transaction.
   * 
   * @author Zhen Zhao, ICT, CAS
   * 
   */
  private static class Commit implements Iterable<Map.Entry<byte[], Boolean>> {
    final NavigableMap<byte[], Boolean> puts = new TreeMap<byte[], Boolean>(
        Bytes.BYTES_COMPARATOR);
    final HTableInterface table;

    Commit(final HTableInterface table) {
      this.table = table;
    }

    void add(byte[] row, boolean isDelete) {
      puts.put(row, isDelete);
    }

    @Override
    public Iterator<Entry<byte[], Boolean>> iterator() {
      return puts.entrySet().iterator();
    }

  }

  /**
   * Reporter class to update the timestamp of this transaction periodically for
   * client failure detection.
   * 
   * @author Zhen Zhao, ICT, CAS
   * 
   */
  private class LivenessReporter extends Thread {

    static final long INTERVAL = DominoConst.TRX_EXPIRED / 2;

    LivenessReporter() {
      super();
      this.setDaemon(true);
    }

    @Override
    public void run() {
      while (readyToCommit) {
        try {
          Thread.sleep(INTERVAL);
        }
        catch (InterruptedException ie) {
          break;
        }
        touch();
      }
    }
  }

  private LivenessReporter reporter = new LivenessReporter();

  // private final TidClient tidClient;
  private final HTableInterface metaTable;
  private final Configuration conf;
  private final long startId;
  private final byte[] startIdBytes;
  private long commitId;
  private final Map<byte[], Commit> commits = new TreeMap<byte[], Commit>(
      Bytes.BYTES_COMPARATOR);
  private final Map<byte[], HTableInterface> tables = new TreeMap<byte[], HTableInterface>(
      Bytes.BYTES_COMPARATOR);

  private boolean readyToCommit = true;
  private boolean clearTrasactionStatus = true;

  protected RWTransaction(Configuration conf, DominoIdIface tidClient)
      throws IOException {
    // this.tidClient = tidClient;
    this.conf = conf;
    this.metaTable = new HTable(conf, DominoConst.TRANSACTION_META);
    this.startId = tidClient.getId();
    this.startIdBytes = DominoConst.long2TranscationRowKey(startId);
    createTransactionMeta();
    startReporter();
  }

  private void startReporter() {
    reporter.start();
  }

  private void createTransactionMeta() throws IOException {
    Put put = new Put(startIdBytes);
    put.add(DominoConst.TRANSACTION_META_FAMILY,
        DominoConst.TRANSACTION_STATUS, startId, DominoConst.TRX_ACTIVE_B);
    put.add(DominoConst.TRANSACTION_META_FAMILY, DominoConst.TRANSACTION_TS,
        startId, DominoConst.tsToBytes(System.currentTimeMillis()));
    metaTable.put(put);
    // metaTable.flushCommits();
  }

  private void touch() {
    Put put = new Put(startIdBytes);
    put.add(DominoConst.TRANSACTION_META_FAMILY, DominoConst.TRANSACTION_TS,
        startId, DominoConst.tsToBytes(System.currentTimeMillis()));
    try {
      metaTable.put(put);
    }
    catch (IOException ioe) {
      Log.warn("Error reporting liveness: {}", ioe.toString());
    }
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
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
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

  public void putStateful(Put put, byte[] table) throws IOException {
    putStateful(put, getTable(table));
  }

  public void putStateful(Put put, HTableInterface table) throws IOException {
    checkIfReadyToContinue();
    try {
      DominoIface iface = table.coprocessorProxy(DominoIface.class,
          put.getRow());
      DResult res = iface.put(put, startId, true);
      if (res != null) {
        throw new IOException(res.getErrMsg());
      }
      addCommit(put.getRow(), false, table);
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
      throw new IOException(t);
    }
  }

  public void put(Put put, byte[] table) throws IOException {
    put(put, getTable(table));
  }

  public void put(Put put, HTableInterface table) throws IOException {
    checkIfReadyToContinue();
    try {
      DominoIface iface = table.coprocessorProxy(DominoIface.class,
          put.getRow());
      DResult res = iface.put(put, startId, false);
      if (res != null) {
        throw new IOException(res.getErrMsg());
      }
      addCommit(put.getRow(), false, table);
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
      throw new IOException(t);
    }
  }

  public void delete(byte[] row, byte[] table) throws IOException {
    delete(row, getTable(table));
  }

  public void delete(byte[] row, HTableInterface table) throws IOException {
    checkIfReadyToContinue();
    try {
      DominoIface iface = table.coprocessorProxy(DominoIface.class, row);
      iface.delete(row, startId);
      addCommit(row, true, table);
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
      throw new IOException(t);
    }
  }

  public void commit() throws IOException {
    checkIfReadyToContinue();
    getCommitId();
    commitPuts();
    removeTransactionStatus();
    closeAllTables();
    reporter.interrupt();
  }

  public void rollback() throws IOException {
    readyToCommit = false;
    transactionMetaAbort();
    rollbackPuts();
    removeTransactionStatus();
    closeAllTables();
    reporter.interrupt();
  }

  private void rollbackPuts() {
    for (byte[] key : commits.keySet()) {
      Commit commit = commits.get(key);
      for (Entry<byte[], Boolean> entry : commit) {
        byte[] row = entry.getKey();
        try {
          commit.table.coprocessorProxy(DominoIface.class, row).rollbackRow(
              row, startId);
        }
        catch (Throwable t) {
          // Wait for other threads to clean the status.
          // So transaction metadata shouldn't be cleared.
          clearTrasactionStatus = false;
        }
      }
    }
  }

  private void transactionMetaAbort() throws IOException {
    try {
      metaTable.coprocessorProxy(TMetaIface.class, startIdBytes)
          .abortTransaction(startIdBytes);
    }
    catch (IOException e) {
      throw e;
    }
    catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public void disable() {
    readyToCommit = false;
  }

  public void checkIfReadyToContinue() throws IOException {
    if (!readyToCommit) {
      throw new IOException(
          "This Transaction has to be aborted because of some earlier failure.");
    }
  }

  private void removeTransactionStatus() {
    if (!clearTrasactionStatus) return;
    Delete delete = new Delete(startIdBytes);
    try {
      metaTable.delete(delete);
    }
    catch (IOException e) {
    }
  }

  private void addCommit(byte[] row, boolean isDelete, HTableInterface table)
      throws IOException {
    Commit commit = commits.get(table.getTableName());
    if (commit == null) {
      commit = new Commit(getTable(table.getTableName()));
      commits.put(table.getTableName(), commit);
    }
    commit.add(row, isDelete);
  }

  private void commitPuts() {
    for (byte[] key : commits.keySet()) {
      Commit commit = commits.get(key);
      for (Entry<byte[], Boolean> entry : commit) {
        byte[] row = entry.getKey();
        try {
          commit.table.coprocessorProxy(DominoIface.class, row).commitRow(row,
              startId, commitId, entry.getValue());
        }
        catch (Throwable t) {
          // Maybe print some log here. It doesn't matter when failed.
          clearTrasactionStatus = false;
        }
      }
    }
  }

  private void getCommitId() throws IOException {
    try {
      long _commitId = metaTable.coprocessorProxy(TMetaIface.class,
          startIdBytes).commitTransaction(startIdBytes);
      if (_commitId == DominoConst.ERR_TRX_ABORTED) {
        throw new IOException("Transaction has been aborted.");
      }
      commitId = _commitId;
    }
    catch (IOException e) {
      readyToCommit = false;
      throw e;
    }
    catch (Throwable t) {
      readyToCommit = false;
      throw new IOException(t);
    }
  }

  @Override
  public long getStartId() {
    return startId;
  }

}

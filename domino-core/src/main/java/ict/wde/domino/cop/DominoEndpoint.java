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

package ict.wde.domino.cop;

import ict.wde.domino.common.Columns;
import ict.wde.domino.common.Columns.Column;
import ict.wde.domino.common.DominoConst;
import ict.wde.domino.common.DominoIface;
import ict.wde.domino.common.HTableWrapper;
import ict.wde.domino.common.InvalidRowStatusException;
import ict.wde.domino.common.MVCC;
import ict.wde.domino.common.TransactionOutOfDateException;
import ict.wde.domino.common.Version;
import ict.wde.domino.common.writable.DResult;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Domino Endpoint: Server side implementation.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class DominoEndpoint implements DominoIface, HTableWrapper {

  static final Logger LOG = LoggerFactory.getLogger(DominoEndpoint.class);

  private CoprocessorEnvironment env = null;
  private HRegion region;
  private final AtomicReference<HTableInterface> metaTable = new AtomicReference<HTableInterface>(
      null);

  private Configuration conf = null;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    LOG.info("-------------DominoEndpoint starting, version:{} ------------",
        Version.VERSION);
    this.env = env;
    conf = env.getConfiguration();
    this.region = ((RegionCoprocessorEnvironment) env).getRegion();
    try {
      getTrxMetaTable();
    }
    catch (IOException e) {
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    HTableInterface meta = metaTable.get();
    if (meta != null) meta.close();
    // this.env = null;
    // this.conf = null;
    this.region = null;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2)
      throws IOException {
    return new ProtocolSignature(Version.VERSION, null);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return Version.VERSION;
  }

  @SuppressWarnings("deprecation")
  @Override
  public DResult get(Get get, long startId) throws IOException {
    if (get.hasFamilies()) get.addFamily(DominoConst.INNER_FAMILY);
    get.setTimeRange(0, startId + 1); // [x, y)
    get.setMaxVersions();
    Result preRead = region.get(get);
    List<KeyValue> status = preRead.getColumn(DominoConst.INNER_FAMILY,
        DominoConst.STATUS_COL);
    if (status == null || status.size() == 0) {
      Result ret = MVCC.handleResult(this, getTrxMetaTable(), preRead, startId,
          null);
      return new DResult(ret, null);
    }
    Integer lockId = region.getLock(null, get.getRow(), true);
    try {
      Result r = MVCC.handleResult(this, getTrxMetaTable(),
          region.get(get, lockId), startId, lockId);
      return new DResult(r, null);
    }
    catch (TransactionOutOfDateException oode) {
      return new DResult(null, oode.getMessage());
    }
    catch (InvalidRowStatusException e) {
      return new DResult(null, e.getMessage());
    }
    finally {
      region.releaseRowLock(lockId);
    }
  }

  private void mutateRow(Mutation mut, Integer lockId) throws IOException {
    @SuppressWarnings("unchecked")
    Pair<Mutation, Integer> pair[] = new Pair[1];
    mut.setWriteToWAL(true);
    pair[0] = new Pair<Mutation, Integer>(mut, lockId);
    region.batchMutate(pair);
  }

  @Override
  public DResult put(Put put, long startId, boolean locking) throws IOException {
    Integer lockId = region.getLock(null, put.getRow(), true);
    try {
      byte[] columnsWritten = MVCC.writeCheckRowStatus(this, getTrxMetaTable(),
          put.getRow(), locking, lockId, startId);
      Put innerPut = clonePut(put, startId, locking, columnsWritten);
      mutateRow(innerPut, lockId);
      return null;
    }
    catch (InvalidRowStatusException e) {
      return new DResult(null, e.getMessage());
    }
    finally {
      region.releaseRowLock(lockId);
    }
  }

  @Override
  public DResult delete(byte[] row, long startId) throws IOException {
    Integer lockId = region.getLock(null, row, true);
    try {
      byte[] columnsWritten = MVCC.writeCheckRowStatus(this, getTrxMetaTable(),
          row, false, lockId, startId);
      Put deletePut = deletePut(row, startId, columnsWritten);
      mutateRow(deletePut, lockId);
      return null;
    }
    catch (InvalidRowStatusException e) {
      return new DResult(null, e.getMessage());
    }
    finally {
      region.releaseRowLock(lockId);
    }
  }

  @Override
  public void rollbackRow(byte[] row, long startId) throws IOException {
    Integer lockId = region.getLock(null, row, true);
    try {
      this.rollbackRow(row, startId, lockId);
    }
    finally {
      region.releaseRowLock(lockId);
    }
  }

  @Override
  public void commitRow(byte[] row, long startId, long commitId,
      boolean isDelete) throws IOException {
    Integer lockId = region.getLock(null, row, true);
    try {
      commitRow(row, startId, commitId, isDelete, lockId);
    }
    finally {
      region.releaseRowLock(lockId);
    }
  }

  private static Put deletePut(byte[] row, long startId, byte[] columnsWritten)
      throws IOException {
    Columns cols = new Columns(columnsWritten);
    Put ret = new Put(row);
    byte[] status = new byte[1 + Bytes.SIZEOF_LONG];
    status[0] = DominoConst.S_DELETE_BYTE;
    Bytes.putLong(status, 1, startId);
    ret.add(DominoConst.INNER_FAMILY, DominoConst.COLUMNS_COL, startId,
        cols.toByteArray());
    ret.add(DominoConst.INNER_FAMILY, DominoConst.STATUS_COL, startId, status);
    return ret;
  }

  private static Put clonePut(Put put, long startId, boolean locking,
      byte[] columnsWritten) {
    Put ret = new Put(put.getRow());
    Map<byte[], List<KeyValue>> families = put.getFamilyMap();
    Columns cols = new Columns(columnsWritten);
    for (byte[] family : families.keySet()) {
      List<KeyValue> columns = families.get(family);
      Iterator<KeyValue> it = columns.iterator();
      while (it.hasNext()) {
        KeyValue kv = it.next();
        // byte[] column = DominoConst.getColumnKey(kv.getQualifier(), startId);
        byte[] qualifier = kv.getQualifier();
        ret.add(family, qualifier, startId, kv.getValue());
        cols.add(family, qualifier);
      }
    }
    Map<String, byte[]> attributes = put.getAttributesMap();
    for (String key : attributes.keySet()) {
      ret.setAttribute(key, attributes.get(key));
    }
    byte[] state = new byte[1 + Bytes.SIZEOF_LONG];
    state[0] = locking ? DominoConst.S_STATEFUL_BYTE
        : DominoConst.S_STATELESS_BYTE;
    Bytes.putLong(state, 1, startId);
    ret.add(DominoConst.INNER_FAMILY, DominoConst.COLUMNS_COL, startId,
        cols.toByteArray());
    ret.add(DominoConst.INNER_FAMILY, DominoConst.STATUS_COL, startId, state);
    return ret;
  }

  private HTableInterface getTrxMetaTable() throws IOException {
    HTableInterface meta = metaTable.get();
    if (meta != null) {
      return meta;
    }
    synchronized (metaTable) {
      meta = metaTable.get();
      if (meta != null) {
        return meta;
      }
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (!admin.tableExists(DominoConst.TRANSACTION_META)) {
        while (true) {
          try {
            admin.createTable(DominoConst.TRANSACTION_META_DESCRIPTOR);
          }
          catch (PleaseHoldException phe) {
            LOG.info("Failed to create transaction meta table: Got a PleaseHoldException.");
            try {
              Thread.sleep(200);
            }
            catch (InterruptedException ie) {
              break;
            }
            continue;
          }
          catch (IOException e) {
            LOG.warn("Failed to create transaction meta table. ", e);
          }
          break;
        }
      }
      admin.close();
      try {
        meta = env.getTable(DominoConst.TRANSACTION_META
            .getBytes(DominoConst.META_CHARSET));
        metaTable.set(meta);
      }
      catch (IOException e) {
        LOG.error("Failed to open transaction meta table: {}.", e.toString());
        throw e;
      }
    }
    return meta;
  }

  public Result get(Get get) throws IOException {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Result get(Get get, Integer lockId) throws IOException {
    return region.get(get, lockId);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rollbackRow(byte[] row, long startId, Integer lockId)
      throws IOException {
    byte[] family = DominoConst.INNER_FAMILY;
    Get get = new Get(row);
    get.setTimeStamp(startId);
    get.addFamily(family);
    Result r = region.get(get, lockId);
    if (r == null || r.isEmpty()) return;
    byte[] colBytes = r.getValue(family, DominoConst.COLUMNS_COL);
    if (colBytes == null || colBytes.length == 0) return;
    Delete del = new Delete(row);
    Columns cols = new Columns(colBytes);
    for (Column col : cols.cols) {
      del.deleteColumn(col.family, col.qualifier, startId);
    }
    del.deleteColumn(family, DominoConst.COLUMNS_COL, startId);
    del.deleteColumn(family, DominoConst.STATUS_COL, startId);
    mutateRow(del, lockId);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void commitRow(byte[] row, long startId, long commitId,
      boolean isDelete, Integer lockId) throws IOException {
    Get get = new Get(row);
    get.setMaxVersions();
    get.addFamily(DominoConst.INNER_FAMILY);
    Result r = region.get(get, lockId);
    if (!containsStatus(r, startId)) {
      // Other transaction may have committed this row of this version
      LOG.info("Commit: No status found, returning: {}.{}",
          new String(this.getName()), new String(row));
      return;
    }
    List<KeyValue> versions = r.getColumn(DominoConst.INNER_FAMILY,
        DominoConst.VERSION_COL);
    Put commit = new Put(row);
    commit.setWriteToWAL(true);
    boolean isFresh = true;
    if (versions.size() >= DominoConst.MAX_VERSION) {
      // We need to clean the earliest version.
      LOG.info("Commit: rolling version window: {}.{}",
          new String(this.getName()), new String(row));
      isFresh = addClearColumns(commit, versions, r, row, isDelete, commitId,
          startId, lockId);
    }
    KeyValue clearStatusKV = new KeyValue(row, DominoConst.INNER_FAMILY,
        DominoConst.STATUS_COL, startId, KeyValue.Type.Delete);
    commit.add(clearStatusKV);
    byte[] value = DominoConst.versionValue(startId, isDelete);
    if (isFresh) {
      KeyValue commitKV = new KeyValue(row, DominoConst.INNER_FAMILY,
          DominoConst.VERSION_COL, commitId, value);
      commit.add(commitKV);
    }
    // commitNumericModifications(row, startId, lockId, commit);
    mutateRow(commit, lockId);
  }

  private static boolean containsStatus(Result r, long startId) {
    List<KeyValue> status = r.getColumn(DominoConst.INNER_FAMILY,
        DominoConst.STATUS_COL);
    if (status == null) return false;
    for (KeyValue kv : status) {
      if (kv.getTimestamp() == startId) return true;
    }
    return false;
  }

  /**
   * 
   * Clear the data out of version window & write them to the second lowest
   * version.
   * 
   * @param commit
   * @param versions
   * @param r
   * @param row
   * @param isDelete
   * @param commitId
   * @param startId
   * @param lockId
   * @return
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private boolean addClearColumns(Put commit, List<KeyValue> versions,
      Result r, byte[] row, boolean isDelete, long commitId, long startId,
      Integer lockId) throws IOException {
    KeyValue commitKV = new KeyValue(row, DominoConst.INNER_FAMILY,
        DominoConst.VERSION_COL, commitId, DominoConst.versionValue(startId,
            isDelete));
    NavigableSet<KeyValue> orderedVersions = new TreeSet<KeyValue>(
        MVCC.VERSION_KV_COMPARATOR);
    orderedVersions.add(versions.get(versions.size() - 1));
    if (versions.size() >= 2) {
      orderedVersions.add(versions.get(versions.size() - 2));
    }
    orderedVersions.add(commitKV);
    Iterator<KeyValue> it = orderedVersions.descendingIterator();
    KeyValue remove = it.next();
    KeyValue prev = it.next();
    byte[] removeValue = remove.getValue();
    byte[] prevValue = prev.getValue();
    long removeCommitId = remove.getTimestamp();
    long removeStartId = DominoConst.getVersion(removeValue);
    long prevStartId = DominoConst.getVersion(prevValue);
    boolean isFresh = (removeCommitId != commitId);
    Columns removeCols = new Columns(DominoConst.getColumnsAt(r, removeStartId));
    if (!DominoConst.isDeleteVersion(removeValue)
        && !DominoConst.isDeleteVersion(prevValue)) {
      Columns prevCols = new Columns(DominoConst.getColumnsAt(r, prevStartId));
      Get get = new Get(row);
      get.setTimeStamp(removeStartId);
      Result res = region.get(get, lockId);
      for (Column col : removeCols.cols) {
        if (prevCols.contains(col.family, col.qualifier)) {
          continue; // a newer value
        }
        // merge it
        byte[] value = res.getValue(col.family, col.qualifier);
        prevCols.add(col.family, col.qualifier);
        commit.add(col.family, col.qualifier, prevStartId, value);
      }
      commit.add(DominoConst.INNER_FAMILY, DominoConst.COLUMNS_COL,
          prevStartId, prevCols.toByteArray());
    }
    if (isFresh) {
      commit.add(new KeyValue(row, DominoConst.INNER_FAMILY,
          DominoConst.VERSION_COL, removeCommitId, KeyValue.Type.Delete));
    }
    commit.add(new KeyValue(row, DominoConst.INNER_FAMILY,
        DominoConst.COLUMNS_COL, removeStartId, KeyValue.Type.Delete));
    for (Column col : removeCols.cols) {
      commit.add(new KeyValue(row, col.family, col.qualifier, removeStartId,
          KeyValue.Type.Delete));
    }
    return isFresh;
  }

  @Override
  public byte[] getName() {
    return region.getTableDesc().getName();
  }

}

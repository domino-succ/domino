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

package ict.wde.domino.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SUCC model implementation.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class MVCC {

  static final Logger LOG = LoggerFactory.getLogger(MVCC.class);

  public static final Comparator<KeyValue> VERSION_KV_COMPARATOR = new Comparator<KeyValue>() {
    @Override
    public int compare(KeyValue o1, KeyValue o2) {
      return Long.compare(o2.getTimestamp(), o1.getTimestamp());
    }
  };

  /**
   * For read results handling, both on the server side and the client side.
   * 
   * Server side: Lock ID should always be NOT NULL. All status issues will be
   * done on the server side.
   * 
   * Client side: Only for simple result handling in scan operations. Lock ID
   * will be NULL. Status issues will cause to re-get the row and let server
   * side do further things.
   * 
   * @param tableWrapper
   *          DResultScanner(Client side) or DominoEndpoint(Server side)
   * @param metaTable
   *          Transaction meta table
   * @param result
   *          Result to be handled
   * @param startId
   *          Transaction start id
   * @param lockId
   *          Row lock id, NULL on the client side.
   * @return The handled result
   * @throws IOException
   * @throws TransactionOutOfDateException
   * @throws InvalidRowStatusException
   */
  public static Result handleResult(HTableWrapper tableWrapper,
      HTableInterface metaTable, Result result, long startId, Integer lockId)
      throws IOException, TransactionOutOfDateException,
      InvalidRowStatusException {
    if (result.isEmpty()) return result;
    List<KeyValue> statusList = result.getColumn(DominoConst.INNER_FAMILY,
        DominoConst.STATUS_COL); // Returned is ArrayList
    List<KeyValue> versionList = result.getColumn(DominoConst.INNER_FAMILY,
        DominoConst.VERSION_COL);
    if (lockId == null && statusList != null && statusList.size() > 0) {
      // This is a client-side scan that encounters a status data.
      if (isSelfDelete(statusList.get(0), startId)) {
        return new Result();
      }
      return tableWrapper.get(getQuery(result));
    }
    List<KeyValue> committedList;
    committedList = handleStatus(tableWrapper, metaTable, statusList,
        result.getRow(), startId, lockId);
    List<KeyValue> mergedList = mergeVersionedList(committedList, versionList);
    List<KeyValue> retKV = new ArrayList<KeyValue>();
    if (isDeletedRow(statusList, mergedList, startId)) {
      return new Result();
    }
    int verCount = mergedList.size() > DominoConst.MAX_VERSION ? DominoConst.MAX_VERSION
        : mergedList.size();
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result
        .getMap();
    map.remove(DominoConst.INNER_FAMILY);
    for (byte[] family : map.keySet()) {
      Map<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);
      for (byte[] column : familyMap.keySet()) {
        NavigableMap<Long, byte[]> values = familyMap.get(column);
        if (isSelfWrite(values, startId)) {
          retKV
              .add(new KeyValue(result.getRow(), family, column,
                  DominoConst.DEFAULT_DATA_VERSION, values.firstEntry()
                      .getValue()));
          continue;
        }
        boolean gotoNextColumn = (verCount == 0);
        for (int i = 0; i < verCount; ++i) {
          KeyValue verKV = mergedList.get(i);
          byte[] verBytes = verKV.getValue();
          if (DominoConst.isDeleteVersion(verBytes)) {
            gotoNextColumn = true;
            break;
          }
          Long ver = DominoConst.getVersion(verBytes);
          if (values.containsKey(ver)) {
            retKV.add(new KeyValue(result.getRow(), family, column,
                DominoConst.DEFAULT_DATA_VERSION, values.get(ver)));
            gotoNextColumn = true;
            break;
          }
        }
        if (gotoNextColumn) continue;
        byte[] value0 = values.get(DominoConst.DATA_VER0);
        if (value0 != null) {
          // This is a committed data out of the version window, but valid.
          retKV.add(new KeyValue(result.getRow(), family, column,
              DominoConst.DEFAULT_DATA_VERSION, value0));
        }
      }
    }
    if (retKV.size() == 0) {
      checkIfTransactionOutOfDate(result.getRow(), tableWrapper, startId,
          mergedList, lockId);
    }
    return new Result(retKV);
  }

  /**
   * Row status checking for write operations.
   * 
   * To check if write-write conflict exists.
   * 
   * @param tableWrapper
   * @param metaTable
   * @param row
   * @param locking
   *          If this is a stateful update.
   * @param lockId
   * @param startId
   * @return The columns this thread wrote before. NULL if this thread didn't
   *         update this row.
   * @throws IOException
   *           If IO Exceptions occur.
   * @throws InvalidRowStatusException
   *           If this row is in a invalid status.
   */
  public static byte[] writeCheckRowStatus(HTableWrapper tableWrapper,
      HTableInterface metaTable, byte[] row, boolean locking, Integer lockId,
      long startId) throws IOException, InvalidRowStatusException {
    Get get = new Get(row);
    get.addFamily(DominoConst.INNER_FAMILY);
    get.setMaxVersions();
    boolean retried = false;
    List<KeyValue> versions;
    byte[] columnsWritten = null;
    while (true) {
      Result res = tableWrapper.get(get, lockId);
      if (res == null || res.isEmpty()) return null;
      List<KeyValue> status = res.getColumn(DominoConst.INNER_FAMILY,
          DominoConst.STATUS_COL);
      versions = res.getColumn(DominoConst.INNER_FAMILY,
          DominoConst.VERSION_COL);
      if (status == null || status.size() == 0) break;
      // NULL if this transaction didn't update this row before.
      columnsWritten = DominoConst.getColumnsAt(res, startId);
      if (columnsWritten == null && status.size() >= DominoConst.MAX_VERSION) {
        if (retried) {
          throw new InvalidRowStatusException(String.format(
              "[%s][%s] Too many concurrent writes on this row.", new String(
                  tableWrapper.getName()), new String(row)));
        }
        // Try to clean some committed/aborted status.
        retried = true;
        handleStatus(tableWrapper, metaTable, status, row, startId, lockId);
        continue;
      }
      if (locking && conflicted(status, startId)) {
        if (retried) {
          throw new InvalidRowStatusException(String.format(
              "[%s][%s] Row is in an update status.",
              new String(tableWrapper.getName()), new String(row)));
        }
        retried = true;
        handleStatus(tableWrapper, metaTable, status, row, startId, lockId);
        continue;
      }
      Iterator<KeyValue> it = status.iterator();
      try {
        while (it.hasNext()) {
          if (conflicted(it.next(), startId)) {
            throw new InvalidRowStatusException(String.format(
                "[%s][%s] Row is in a stateful update status.", new String(
                    tableWrapper.getName()), new String(row)));
          }
        }
      }
      catch (InvalidRowStatusException e) {
        if (retried) {
          throw e;
        }
        retried = true;
        handleStatus(tableWrapper, metaTable, status, row, startId, lockId);
        continue;
      }
      break;
    }
    if (locking && versions != null) {
      for (KeyValue version : versions) {
        if (version.getTimestamp() < startId) {
          break;
        }
        throw new InvalidRowStatusException(String.format(
            "[%s][%s] Row has a newer value.",
            new String(tableWrapper.getName()), new String(row)));
      }
    }
    return columnsWritten;
  }

  private static boolean isSelfDelete(KeyValue status, long startId) {
    return status.getTimestamp() == startId
        && DominoConst.isDelete(status.getValue());
  }

  private static boolean isDeletedRow(List<KeyValue> statusList,
      List<KeyValue> versionList, long startId) {
    if (statusList != null && statusList.size() > 0) {
      if (isSelfDelete(statusList.get(0), startId)) {
        return true;
      }
    }
    if (versionList != null && versionList.size() > 0) {
      KeyValue version = versionList.get(0);
      if (DominoConst.isDeleteVersion(version.getValue())) {
        return true;
      }
    }
    return false;
  }

  private static boolean isSelfWrite(NavigableMap<Long, byte[]> values,
      long startId) {
    if (values == null || values.size() == 0) return false;
    return values.firstKey() == startId;
  }

  private static void checkIfTransactionOutOfDate(byte[] row,
      HTableWrapper tableWrapper, long startId,
      List<KeyValue> mergedVersionList, Integer lockId)
      throws TransactionOutOfDateException, IOException {
    int verCount = DominoConst.MAX_VERSION < mergedVersionList.size() ? DominoConst.MAX_VERSION
        : mergedVersionList.size();
    if (verCount == DominoConst.MAX_VERSION
        && mergedVersionList.get(verCount - 1).getTimestamp() > startId) {
      throw new TransactionOutOfDateException("Transaction out of date.");
    }
    if (verCount > 0) return;
    Get allVer = new Get(row);
    allVer.addColumn(DominoConst.INNER_FAMILY, DominoConst.VERSION_COL);
    allVer.setMaxVersions();
    Result res = tableWrapper.get(allVer, lockId);
    List<KeyValue> vers = res.getColumn(DominoConst.INNER_FAMILY,
        DominoConst.VERSION_COL);
    if (vers.size() >= DominoConst.MAX_VERSION
        && vers.get(vers.size() - 1).getTimestamp() > startId) {
      throw new TransactionOutOfDateException("Transaction out of date.");
    }
  }

  private static Get getQuery(Result r) {
    Get get = new Get(r.getRow());
    get.setMaxVersions();
    for (byte[] family : r.getMap().keySet()) {
      if (r.getMap().get(family).size() == 0) {
        get.addFamily(family);
        continue;
      }
      for (byte[] column : r.getMap().get(family).keySet()) {
        get.addColumn(family, column);
      }
    }
    return get;
  }

  private static List<KeyValue> handleStatus(HTableWrapper tableWrapper,
      HTableInterface metaTable, List<KeyValue> status, byte[] row,
      long startId, Integer lockId) throws IOException,
      InvalidRowStatusException {
    if (status == null || status.size() == 0) return null;
    NavigableSet<KeyValue> committed = new TreeSet<KeyValue>(
        VERSION_KV_COMPARATOR);
    for (KeyValue kv : status) {
      if (kv.isDelete() || kv.isEmptyColumn()) {
        continue;
      }
      long transactionId = kv.getTimestamp();
      if (transactionId == startId) {
        continue;
      }
      Result tStatus = getTransactionStatus(metaTable, transactionId);
      if (tStatus == null) {
        // There must be a corresponding trx status.
        throw new InvalidRowStatusException("No transaction status found");
      }
      switch (DominoConst.transactionStatus(tStatus)) {
      case DominoConst.TRX_ACTIVE:
        // Expire issues are done by TMetaEndpoint
        continue;
      case DominoConst.TRX_ABORTED:
        rollbackRow(tableWrapper, row, transactionId, lockId);
        break;
      case DominoConst.TRX_COMMITTED:
        long commitId = DominoConst.commitId(tStatus);
        if (commitId > startId) continue; // Ignore the "future" value.
        committed.add(commitRow(tableWrapper, kv, transactionId, commitId,
            lockId));
        break;
      default:
        throw new InvalidRowStatusException("Invalid transaction status");
      }
    }
    return new ArrayList<KeyValue>(committed);
  }

  private static boolean conflicted(KeyValue status, long startId) {
    if (!DominoConst.isStatefulStatus(status.getValue())) {
      return false;
    }
    return status.getTimestamp() != startId;
  }

  private static boolean conflicted(List<KeyValue> status, long startId) {
    if (status.size() > 1) return true;
    if (status.size() == 0) return false;
    return status.get(0).getTimestamp() != startId;
  }

  private static Result getTransactionStatus(HTableInterface metaTable,
      long transactionId) throws IOException {
    byte[] row = DominoConst.long2TranscationRowKey(transactionId);
    return metaTable.coprocessorProxy(TMetaIface.class, row)
        .getTransactionStatus(transactionId);
  }

  private static KeyValue commitRow(HTableWrapper tableWrapper,
      KeyValue statusKV, long startId, long commitId, Integer lockId) {
    byte[] row = statusKV.getRow();
    boolean isDelete = DominoConst.isDelete(statusKV.getValue());
    try {
      tableWrapper.commitRow(row, startId, commitId, isDelete, lockId);
    }
    catch (IOException ioe) {
    }
    byte[] value = DominoConst.versionValue(startId, isDelete);
    KeyValue commitKV = new KeyValue(row, DominoConst.INNER_FAMILY,
        DominoConst.VERSION_COL, commitId, value);
    return commitKV;
  }

  private static void rollbackRow(HTableWrapper tableWrapper, byte[] row,
      long id, Integer lockId) {
    try {
      tableWrapper.rollbackRow(row, id, lockId);
    }
    catch (IOException e) {
      // LOG.warn("Failed to rollback row.", e);
    }
  }

  private static List<KeyValue> mergeVersionedList(List<KeyValue> committed,
      List<KeyValue> version) {
    if (committed == null) return version;
    List<KeyValue> ret = new ArrayList<KeyValue>(committed.size()
        + version.size());
    int i = 0, j = 0;
    while (i < committed.size() || j < version.size()) {
      if (i < committed.size() && j < version.size()) {
        if (VERSION_KV_COMPARATOR.compare(committed.get(i), version.get(j)) < 0) {
          ret.add(committed.get(i++));
        }
        else {
          ret.add(version.get(j++));
        }
      }
      else if (i < committed.size()) {
        ret.add(committed.get(i++));
      }
      else {
        ret.add(version.get(j++));
      }
    }
    return ret;
  }

}

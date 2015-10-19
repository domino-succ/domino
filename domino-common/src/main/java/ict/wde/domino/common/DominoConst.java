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
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Domino Constants definition class.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class DominoConst {

  public static final String ZK_PROP = "hbase.zookeeper.quorum";

  public static final Charset META_CHARSET = Charset.forName("UTF-8");
  public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
  public static final long DEFAULT_DATA_VERSION = 1l;

  // Trasaction metatable --->
  public static final String TRANSACTION_META = "_trx_meta";
  public static final byte[] TRANSACTION_META_FAMILY = "_meta"
      .getBytes(META_CHARSET);
  public static final byte[] TRANSACTION_STATUS = "st".getBytes(META_CHARSET);
  public static final byte[] TRANSACTION_COMMIT_ID = "cid"
      .getBytes(META_CHARSET);
  public static final byte[] TRANSACTION_TS = "ts".getBytes(META_CHARSET);
  public static final HTableDescriptor TRANSACTION_META_DESCRIPTOR;

  public static final byte TRX_ACTIVE = 1;
  public static final byte TRX_COMMITTED = 2;
  public static final byte TRX_ABORTED = 3;

  public static final byte[] TRX_ACTIVE_B = new byte[] { TRX_ACTIVE };
  public static final byte[] TRX_COMMITTED_B = new byte[] { TRX_COMMITTED };
  public static final byte[] TRX_ABORTED_B = new byte[] { TRX_ABORTED };

  public static final long TRX_EXPIRED = 40000;
  public static final long ERR_TRX_ABORTED = -1;

  public static byte[] tsToBytes(long ts) {
    return Bytes.toBytes(ts);
  }

  public static long bytesToTs(byte[] ts) {
    return Bytes.toLong(ts);
  }

  // <--- Trasaction metatable

  public static final byte[] INNER_FAMILY = "_dmn".getBytes(META_CHARSET);
  public static final byte[] STATUS_COL = "_st".getBytes(META_CHARSET);
  public static final byte[] VERSION_COL = "_ver".getBytes(META_CHARSET);
  public static final byte[] COLUMNS_COL = "_col".getBytes(META_CHARSET);
  public static final HColumnDescriptor INNER_FAMILY_DESCRIPTER;
  public static final String COPROCESSOR_CLASS = "ict.wde.domino.cop.DominoEndpoint";

  public static final String TID_EP_TABLE = "_tid";

  public static final int MAX_VERSION = 20;
  public static final int MAX_DATA_VERSION = MAX_VERSION * 2;

  public static final long DATA_VER0 = 1;

  static {
    INNER_FAMILY_DESCRIPTER = new HColumnDescriptor(INNER_FAMILY);
    INNER_FAMILY_DESCRIPTER.setInMemory(true);
    INNER_FAMILY_DESCRIPTER.setMaxVersions(MAX_VERSION + 1);
    TRANSACTION_META_DESCRIPTOR = new HTableDescriptor(TRANSACTION_META);
    try {
      TRANSACTION_META_DESCRIPTOR
          .addCoprocessor("ict.wde.domino.cop.TMetaEndpoint");
    }
    catch (IOException e) {
    }
    HColumnDescriptor metaFamily = new HColumnDescriptor(
        TRANSACTION_META_FAMILY);
    metaFamily.setInMemory(true);
    TRANSACTION_META_DESCRIPTOR.addFamily(metaFamily);
  }

  public static final byte S_STATEFUL_BYTE = 0x1;
  public static final byte S_STATELESS_BYTE = 0x2;
  public static final byte S_DELETE_BYTE = 0x3;

  public static final byte V_NORMAL_BYTE = 0x1;
  public static final byte V_DELETE_BYTE = 0x2;

  public static byte[] versionValue(long startId, boolean isDelete) {
    byte[] ret = new byte[Bytes.SIZEOF_LONG + 1];
    ret[0] = isDelete ? V_DELETE_BYTE : V_NORMAL_BYTE;
    Bytes.putLong(ret, 1, startId);
    return ret;
  }

  public static boolean isDeleteVersion(byte[] value) {
    return value[0] == V_DELETE_BYTE;
  }

  public static boolean isStatefulStatus(byte[] value) {
    if (value == null || value.length < 1) return false;
    return value[0] == S_STATEFUL_BYTE;
  }

  public static boolean isDelete(byte[] value) {
    if (value == null || value.length < 1) return false;
    return value[0] == S_DELETE_BYTE;
  }

  public static long getVersion(byte[] value) {
    return Bytes.toLong(value, 1);
  }

  public static byte[] long2TranscationRowKey(long id) {
    byte[] ret = new byte[Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG];
    short prefix = (short) (id % Short.MAX_VALUE);
    Bytes.putShort(ret, 0, prefix);
    Bytes.putLong(ret, Bytes.SIZEOF_SHORT, id);
    return ret;
  }

  public static long getTidFromTMetaKey(byte[] key) {
    return Bytes.toLong(key, Bytes.SIZEOF_SHORT);
  }

  public static byte transactionStatus(Result r) throws IOException {
    byte[] status = r.getValue(TRANSACTION_META_FAMILY, TRANSACTION_STATUS);
    if (status == null || status.length != 1) {
      throw new IOException("Invalid transaction status");
    }
    return status[0];
  }

  public static long getLastTouched(Result r) {
    return Bytes.toLong(r.getValue(TRANSACTION_META_FAMILY, TRANSACTION_TS));
  }

  public static long commitId(Result r) throws IOException {
    byte[] cid = r.getValue(TRANSACTION_META_FAMILY, TRANSACTION_COMMIT_ID);
    if (cid == null || cid.length != Bytes.SIZEOF_LONG) {
      throw new IOException("Invalid commit id");
    }
    return Bytes.toLong(cid);
  }

  public static byte[] getColumnsAt(Result r, long startId) {
    List<KeyValue> columns = r.getColumn(INNER_FAMILY, COLUMNS_COL);
    for (KeyValue kv : columns) {
      if (kv.getTimestamp() == startId) {
        return kv.getValue();
      }
    }
    return null;
  }

}

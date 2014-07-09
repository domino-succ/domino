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

package ict.wde.domino.id.cop;

import ict.wde.domino.common.Version;
import ict.wde.domino.id.TidDef;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DTO Endpoint server.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class TidEPServer implements TidEPIface {

  static final Logger LOG = LoggerFactory.getLogger(TidEPServer.class);

  private HRegion region;
  private final AtomicLong tid = new AtomicLong(TidDef.START_ID);
  private final AtomicLong preAlloc = new AtomicLong(TidDef.START_ID);

  private boolean initialized = false;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    LOG.info("Domino coprocessor id service starting...");
    this.region = ((RegionCoprocessorEnvironment) env).getRegion();
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    LOG.info("Domino coprocessor id service stopping...");
  }

  @Override
  public long getId() throws IOException {
    checkTidUpperBound();
    return tid.getAndIncrement();
  }

  @Override
  public long[] getId(int batch) throws IOException {
    if (batch <= 0) {
      return null;
    }
    checkTidUpperBound();
    long[] ret = new long[batch];
    for (int i = 0; i < batch; i++) {
      ret[i] = tid.incrementAndGet();
    }
    return ret;
  }

  @Override
  public long getTimeInMillSec() throws IOException {
    return System.currentTimeMillis();
  }

  @Override
  public long[] getTimeInMillSec(int batch) throws IOException {
    if (batch <= 0) {
      return null;
    }
    long[] ret = new long[batch];
    for (int i = 0; i < batch; i++) {
      ret[i] = System.currentTimeMillis();
    }
    return ret;
  }

  @Override
  public void close() {
  }

  private void syncWriteTid() {
    preAlloc.set(tid.get() + TidDef.PRE_ALLOC_BATCH);
    _writeTid();
  }

  private void _writeTid() {
    try {
      byte[] value = Bytes.toBytes(preAlloc.get());
      Put put = new Put(TidDef.EP_ROW);
      put.add(TidDef.EP_FAMILY, TidDef.EP_COLUMN, TidDef.EP_VERSION, value);
      put.setWriteToWAL(true);
      region.put(put);
    }
    catch (Exception e) {
      LOG.error("Error writing pre-allocated tid to HBase.", e);
    }
  }

  private void readTid() {
    try {
      Get get = new Get(TidDef.EP_ROW);
      get.addFamily(TidDef.EP_FAMILY);
      Result res = region.get(get);
      byte[] value = res.getValue(TidDef.EP_FAMILY, TidDef.EP_COLUMN);
      if (value == null || value.length != Bytes.SIZEOF_LONG) {
        LOG.info("Got no pre-alloc tid, resetting to 0.");
        this.tid.set(TidDef.START_ID);
      }
      else {
        long old = Bytes.toLong(value);
        LOG.info("Got pre-alloc tid: {}", old);
        this.tid.set(old);
      }
    }
    catch (Exception e) {
      LOG.info("Error reading pre-alloc tid.", e);
      this.tid.set(TidDef.START_ID);
    }
  }

  private void checkTidUpperBound() {
    if (tid.get() < preAlloc.get()) {
      return;
    }
    synchronized (tid) {
      if (tid.get() < preAlloc.get()) {
        return;
      }
      if (!initialized) {
        /**
         * Note that this cannot be done in start() because region is
         * initialized after that.
         */
        readTid();
        initialized = true;
      }
      syncWriteTid();
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return Version.VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return new ProtocolSignature(Version.VERSION, null);
  }

}

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

import ict.wde.domino.common.DominoConst;
import ict.wde.domino.id.DominoIdIface;
import ict.wde.domino.id.TidDef;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DTO client class.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class TidEPClient implements DominoIdIface {

  static final Logger LOG = LoggerFactory.getLogger(TidEPClient.class);

  private final HTableInterface table;
  private final Configuration conf;
  private final TidEPIface server;

  public TidEPClient(String zkAddr) throws IOException {
    conf = new Configuration();
    conf.set(DominoConst.ZK_PROP, zkAddr);
    table = initTidTable();
    server = table.coprocessorProxy(TidEPIface.class, TidDef.EP_ROW);
  }

  private HTableInterface initTidTable() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    while (true) {
      try {
        if (!admin.tableExists(DominoConst.TID_EP_TABLE)) {
          admin.createTable(TidDef.EP_TABLE_DESCRIPTOR);
        }
        break;
      }
      catch (PleaseHoldException holdex) {
        LOG.info("Got a PleaseHoldException, wait & retry...");
        try {
          Thread.sleep(100);
        }
        catch (InterruptedException ie) {
          break;
        }
        continue;
      }
      catch (IOException ioe) {
        LOG.error("Error creating Tid table.", ioe);
        break;
      }
    }
    admin.close();
    return new HTable(conf, DominoConst.TID_EP_TABLE);
  }

  @Override
  public void close() throws IOException {
    table.close();
  }

  @Override
  public long getId() throws IOException {
    return server.getId();
  }

  @Override
  public long[] getId(int batch) throws IOException {
    return server.getId(batch);
  }

  @Override
  public long getTimeInMillSec() throws IOException {
    return server.getTimeInMillSec();
  }

  @Override
  public long[] getTimeInMillSec(int batch) throws IOException {
    return server.getTimeInMillSec(batch);
  }

}

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

  public TidEPClient(Configuration config) throws IOException {
    conf = config;
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

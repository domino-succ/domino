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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

/**
 * Table Wrapper Interface used by MVCC control.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public interface HTableWrapper {

  /**
   * To get the table name.
   * 
   * @return
   */
  public byte[] getName();

  /**
   * To get a row.
   * 
   * @param get
   * @return
   * @throws IOException
   */
  public Result get(Get get) throws IOException;

  /**
   * To get a row using lockId.
   * 
   * @param get
   * @param lockId
   * @return
   * @throws IOException
   */
  public Result get(Get get, Integer lockId) throws IOException;

  /**
   * To rollback a row.
   * 
   * @param row
   * @param startId
   * @param lockId
   * @throws IOException
   */
  public void rollbackRow(byte[] row, long startId, Integer lockId)
      throws IOException;

  /**
   * To commit a row.
   * 
   * @param row
   * @param startId
   * @param commitId
   * @param isDelete
   * @param lockId
   * @throws IOException
   */
  public void commitRow(byte[] row, long startId, long commitId,
      boolean isDelete, Integer lockId) throws IOException;

}

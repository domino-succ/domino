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

import ict.wde.domino.common.writable.DResult;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Domino Endpoint interface.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public interface DominoIface extends Coprocessor, CoprocessorProtocol {

  /**
   * To get a row.
   * 
   * @param get
   * @param startId
   * @return
   * @throws IOException
   */
  public DResult get(Get get, long startId) throws IOException;

  /**
   * To update a row.
   * 
   * @param put
   * @param startId
   * @param locking
   * @return
   * @throws IOException
   */
  public DResult put(Put put, long startId, boolean locking) throws IOException;

  /**
   * To delete a row.
   * 
   * ** Deletion will delete the entire row instead of part of the columns.
   * 
   * @param row
   * @param startId
   * @return
   * @throws IOException
   */
  public DResult delete(byte[] row, long startId) throws IOException;

  /**
   * To rollback a row.
   * 
   * @param row
   * @param startId
   * @throws IOException
   */
  public void rollbackRow(byte[] row, long startId) throws IOException;

  /**
   * To commit a row.
   * 
   * @param row
   * @param startId
   * @param commitId
   * @param isDelete
   * @throws IOException
   */
  public void commitRow(byte[] row, long startId, long commitId,
      boolean isDelete) throws IOException;

}

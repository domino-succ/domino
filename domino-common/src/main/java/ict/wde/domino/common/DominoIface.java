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

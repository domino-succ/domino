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

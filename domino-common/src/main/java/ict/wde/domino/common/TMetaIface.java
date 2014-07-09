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

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Transaction Metadata Endpoint Interface.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public interface TMetaIface extends Coprocessor, CoprocessorProtocol {

  /**
   * To get the status of a transaction specified by the startId.
   * 
   * @param startId
   * @return
   * @throws IOException
   */
  public Result getTransactionStatus(long startId) throws IOException;

  /**
   * To commit a transaction metadata, returning the commit id.
   * 
   * If the transaction is aborted, return -1.
   * 
   * @param startId
   * @return
   * @throws IOException
   */
  public long commitTransaction(byte[] startId) throws IOException;

  /**
   * To abort a transaction metadata.
   * 
   * @param startId
   * @throws IOException
   */
  public void abortTransaction(byte[] startId) throws IOException;

}

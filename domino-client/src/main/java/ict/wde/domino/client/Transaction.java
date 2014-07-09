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

package ict.wde.domino.client;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Interface that provides all transaction functions.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public interface Transaction {

  /**
   * To commit the transaction.
   * 
   * @throws IOException
   *           when connection failure or transaction commit failure occurs.
   */
  public void commit() throws IOException;

  /**
   * To rollback the transaction.
   * 
   * @throws IOException
   *           when connection failure occurs.
   */
  public void rollback() throws IOException;

  /**
   * To see if the transaction could continue to read/write/commit.
   * 
   * A transaction will be disabled when read/write/commit failure occurs and
   * must be rolled-back.
   * 
   * If this transaction is still available, nothing will happen.
   * 
   * @throws IOException
   *           when transaction is disabled.
   */
  public void checkIfReadyToContinue() throws IOException;

  /**
   * To disable a transaction.
   */
  public void disable();

  /**
   * To get the start id of this transaction.
   * 
   * @return Long type start id.
   */
  public long getStartId();

  /**
   * To get data from Domino using a table name.
   * 
   * @param get
   *          A HBase Get instance.
   * @param table
   *          The table name.
   * @return
   * @throws IOException
   *           when connection failure or read failure occurs.
   */
  public Result get(Get get, byte[] table) throws IOException;

  /**
   * To get data from Domino using a HTableInterface instance.
   * 
   * @param get
   * @param table
   *          A HTableInterface instance.
   * @return
   * @throws IOException
   *           when connection failure or read failure occurs.
   */
  public Result get(Get get, HTableInterface table) throws IOException;

  /**
   * To scan data using a table name.
   * 
   * @param scan
   *          A HBase Scan instance.
   * @param table
   *          A table name.
   * @return
   * @throws IOException
   *           when connection failure or read failure occurs.
   */
  public ResultScanner scan(Scan scan, byte[] table) throws IOException;

  /**
   * To scan data using a HTableInterface instance.
   * 
   * @param scan
   * @param table
   *          A HTableInterface instance.
   * @return
   * @throws IOException
   *           when connection failure or read failure occurs.
   */
  public ResultScanner scan(Scan scan, HTableInterface table)
      throws IOException;

  /**
   * To put a stateful data into Domino using a table name.
   * 
   * A stateful data means values in this put is based on earlier results read
   * in this transaction.
   * 
   * @param put
   *          A HBase Put instance.
   * @param table
   *          A table name.
   * @throws IOException
   *           when connection failure or put failure occurs.
   */
  public void putStateful(Put put, byte[] table) throws IOException;

  /**
   * To put a stateful data into Domino using a HTableInterface instance.
   * 
   * @param put
   *          A HBase Put instance.
   * @param table
   *          A HTableInterface instance.
   * @throws IOException
   *           when connection failure or put failure occurs.
   */
  public void putStateful(Put put, HTableInterface table) throws IOException;

  /**
   * To put a stateless data into Domino using a table name.
   * 
   * A stateless data means values in this put are brand new and irrelevant to
   * the historical results read by this transaction.
   * 
   * @param put
   *          A HBase Put instance.
   * @param table
   *          A table name.
   * @throws IOException
   *           when connection failure or put failure occurs.
   */
  public void put(Put put, byte[] table) throws IOException;

  /**
   * To put a stateless data into Domino using a HTableInterface instance.
   * 
   * @param put
   * @param table
   *          A HTableInterface instance
   * @throws IOException
   *           when connection failure or put failure occurs.
   */
  public void put(Put put, HTableInterface table) throws IOException;

  /**
   * To delete a row using a table name.
   * 
   * @param row
   * @param table
   * @throws IOException
   *           when connection failure or deletion failure occurs.
   */
  public void delete(byte[] row, byte[] table) throws IOException;

  /**
   * To delete a row using a HTableInterface instance.
   * 
   * @param row
   * @param table
   * @throws IOException
   *           when connection failure or deletion failure occurs.
   */
  public void delete(byte[] row, HTableInterface table) throws IOException;

}

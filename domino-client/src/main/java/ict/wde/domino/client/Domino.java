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

import ict.wde.domino.common.Columns;
import ict.wde.domino.common.DominoConst;
import ict.wde.domino.id.DominoIdIface;
import ict.wde.domino.id.DominoIdService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Domino client that provides table management & transaction creation
 * interface.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class Domino implements Closeable {

  static final Logger LOG = LoggerFactory.getLogger(Domino.class);

  private final Configuration config;
  private final DominoIdIface tidClient;
  private final HBaseAdmin admin;

  /**
   * Constructor with a ZooKeeper connection string.
   * 
   * @param zookeeperAddress
   *          e.g. host1:port1,host2:port2,host3:port3...
   * @throws IOException
   *           when connection failure occurs.
   */
  public Domino(String zookeeperAddress) throws IOException {
    config = new Configuration();
    config.set(DominoConst.ZK_PROP, zookeeperAddress);
    try {
      tidClient = newTidClient(zookeeperAddress);
    }
    catch (IOException e) {
      LOG.error("Error connecting to domino id service. ", e);
      throw e;
    }
    admin = new HBaseAdmin(config);
    initInnerTables();
    // tableMeta = new HTable(config, DominoConst.TABLE_META);
  }

  /**
   * Constructor with a org.apache.hadoop.conf.Configuration instance which may
   * contain the location of ZooKeeper.
   * 
   * @param config
   *          Instance of the Configuration class
   * @throws IOException
   *           when connection failure occurs.
   */
  public Domino(Configuration config) throws IOException {
    this.config = config;
    try {
      tidClient = newTidClient(config.get(DominoConst.ZK_PROP));
    }
    catch (IOException e) {
      LOG.error("Error connecting to domino id service. ", e);
      throw e;
    }
    admin = new HBaseAdmin(config);
    initInnerTables();
  }

  /**
   * Start a transaction that can read/write data.
   * 
   * @return A Transaction instance which provides interface to read/write.
   * @throws IOException
   *           when connection failure occurs.
   */
  public Transaction startTransaction() throws IOException {
    return new RWTransaction(config, tidClient);
  }

  /**
   * Start a read-only transaction.
   * 
   * @return A Transaction instance which can only read(get or scan) data.
   * @throws IOException
   *           when connection failure occurs.
   */
  public Transaction startReadOnlyTransaction() throws IOException {
    return new ReadOnlyTransaction(config, tidClient);
  }

  /**
   * Check if the table exists.
   * 
   * @param tableName
   * @return true if table exists.
   * @throws IOException
   *           when connection failure occurs.
   */
  public boolean tableExists(String tableName) throws IOException {
    return admin.tableExists(tableName);
  }

  /**
   * Check if the table exists.
   * 
   * @param tableName
   * @return true if table exists.
   * @throws IOException
   *           when connection failure occurs.
   */
  public boolean tableExists(byte[] tableName) throws IOException {
    return admin.tableExists(tableName);
  }

  /**
   * Drop a table.
   * 
   * @param tableName
   * @throws IOException
   *           when connection failure occurs.
   */
  public void dropTable(String tableName) throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /**
   * Drop a table.
   * 
   * @param tableName
   * @throws IOException
   */
  public void dropTable(byte[] tableName) throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /**
   * Create a table with the table descriptor.
   * 
   * This method will add a Domino inner Column Family and a Domino Endpoint to
   * the descriptor.
   * 
   * This method MUST be used to create the tables on which Domino features will
   * be needed.
   * 
   * @param table
   *          An instance of HBase HTableDescriptor class.
   * @throws IOException
   *           if creation failure occurs.
   */
  public void createTable(HTableDescriptor table) throws IOException {
    wrapTable(table);
    admin.createTable(table);
  }

  /**
   * Create a table with the table descriptor and splitKeys.
   * 
   * See more details about splitKeys in the corresponding method of HBaseAdmin
   * class.
   * 
   * @param table
   * @param splitKeys
   * @throws IOException
   *           if creation failure occurs.
   */
  public void createTable(HTableDescriptor table, byte[][] splitKeys)
      throws IOException {
    wrapTable(table);
    admin.createTable(table, splitKeys);
  }

  /**
   * Create a table with the table descriptor and startKey/endKey/numRegions.
   * 
   * See more details about splitKeys in the corresponding method of HBaseAdmin
   * class.
   * 
   * @param table
   * @param startKey
   * @param endKey
   * @param numRegions
   * @throws IOException
   *           if creation failure occurs.
   */
  public void createTable(HTableDescriptor table, byte[] startKey,
      byte[] endKey, int numRegions) throws IOException {
    wrapTable(table);
    admin.createTable(table, startKey, endKey, numRegions);
  }

  private static void wrapTable(HTableDescriptor table) throws IOException {
    table.addFamily(DominoConst.INNER_FAMILY_DESCRIPTER);
    table.addCoprocessor(DominoConst.COPROCESSOR_CLASS);
    for (HColumnDescriptor cd : table.getColumnFamilies()) {
      cd.setMaxVersions(DominoConst.MAX_DATA_VERSION);
    }
  }

  /**
   * To close the Domino client.
   */
  @Override
  public void close() throws IOException {
    admin.close();
  }

  /**
   * To get the instance of Configuration.
   * 
   * @return
   */
  public Configuration config() {
    return config;
  }

  private DominoIdIface newTidClient(String zookeeperAddress)
      throws IOException {
    return DominoIdService.getClient(zookeeperAddress);
  }

  private void initInnerTables() throws IOException {
    if (!admin.tableExists(DominoConst.TRANSACTION_META)) {
      try {
        admin.createTable(DominoConst.TRANSACTION_META_DESCRIPTOR);
      }
      catch (IOException e) {
      }
    }
  }

  /**
   * A public call with better performance to load data into a table, adding
   * Domino inner columns to make Domino Transaction features available.
   * 
   * The table MUST be created by the interface of this class before.
   * 
   * @param put
   *          A HBase Put instance.
   * @param table
   *          A HTableInterface instance.
   * @throws IOException
   */
  public static void loadData(Put put, HTableInterface table)
      throws IOException {
    table.put(clonePut(put));
  }

  /**
   * Batch version to load data.
   * 
   * @param puts
   * @param table
   * @throws IOException
   */
  public static void loadData(List<Put> puts, HTableInterface table)
      throws IOException {
    List<Put> clone = new ArrayList<Put>(puts.size());
    for (Put put : puts) {
      clone.add(clonePut(put));
    }
    table.put(clone);
  }

  private static Put clonePut(Put put) {
    long startId = 0;
    long commitId = 1;
    Put ret = new Put(put.getRow());
    Map<byte[], List<KeyValue>> families = put.getFamilyMap();
    Columns cols = new Columns(null);
    for (byte[] family : families.keySet()) {
      List<KeyValue> columns = families.get(family);
      Iterator<KeyValue> it = columns.iterator();
      while (it.hasNext()) {
        KeyValue kv = it.next();
        // byte[] column = DominoConst.getColumnKey(kv.getQualifier(), startId);
        byte[] qualifier = kv.getQualifier();
        ret.add(family, qualifier, startId, kv.getValue());
        cols.add(family, qualifier);
      }
    }
    Map<String, byte[]> attributes = put.getAttributesMap();
    for (String key : attributes.keySet()) {
      ret.setAttribute(key, attributes.get(key));
    }
    ret.add(DominoConst.INNER_FAMILY, DominoConst.COLUMNS_COL, startId,
        cols.toByteArray());
    ret.add(DominoConst.INNER_FAMILY, DominoConst.VERSION_COL, commitId,
        DominoConst.versionValue(startId, false));
    return ret;
  }

}

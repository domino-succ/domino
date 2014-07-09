package ict.wde.domino.client.test;

import ict.wde.domino.client.Domino;
import ict.wde.domino.client.Transaction;
import ict.wde.domino.console.Env;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class InsertData {

  public static void main(String[] args) throws Exception {
    long TOTAL = 500;
    byte[] cf = "test-cf1".getBytes();
    byte[] col = "test-col1".getBytes();

    String hbaseConf = Env.HBASE_SITE;
    if (args.length > 0) hbaseConf = args[0];

    Configuration conf = new Configuration();
    conf.addResource(new Path(hbaseConf));
    HTable table = new HTable(conf, "dom-test");
    Domino dom = new Domino(conf);
    long startts = System.currentTimeMillis();
    Transaction trx = dom.startTransaction();
    for (int i = 0; i < TOTAL; ++i) {
      Put put = new Put(String.format("KEY_%05d", i).getBytes());
      put.add(cf, col, String.format("hello_%05d", i).getBytes());
      trx.put(put, table);
    }
    trx.commit();
    System.out.println("Transactional: " + TOTAL * 1000.0
        / (System.currentTimeMillis() - startts));
    dom.close();
    startts = System.currentTimeMillis();
    for (int i = 0; i < TOTAL; ++i) {
      // table.get(new Get(String.format("KEY_%05d", i).getBytes()));
      Put put = new Put(String.format("KEY_%05d", i).getBytes());
      put.add(cf, col, String.format("hello_%05d", i).getBytes());
      table.put(put);
      table.flushCommits();
    }
    System.out.println("Non-Transactional: " + TOTAL * 1000.0
        / (System.currentTimeMillis() - startts));
    table.close();
  }
}

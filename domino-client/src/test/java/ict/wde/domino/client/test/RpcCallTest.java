package ict.wde.domino.client.test;

import ict.wde.domino.client.Domino;
import ict.wde.domino.client.Transaction;
import ict.wde.domino.console.Env;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

public class RpcCallTest {

  static final AtomicLong count = new AtomicLong();

  public static void main(String[] args) throws Exception {

    String hbaseConf = Env.HBASE_SITE;
    if (args.length > 0) hbaseConf = args[0];

    Configuration conf = new Configuration();
    conf.addResource(new Path(hbaseConf));
    HTable table = new HTable(conf, "dom-test");
    Domino dom = new Domino(conf);
    Transaction trx = dom.startTransaction();
    Worker worker[] = new Worker[100];
    for (int i = 0; i < worker.length; ++i) {
      worker[i] = new Worker(trx, table);
      worker[i].start();
    }
    long t0 = System.currentTimeMillis();
    while (true) {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        break;
      }
      long ct = count.get();
      System.out.println(ct * 1000.0 / (System.currentTimeMillis() - t0));
      t0 = System.currentTimeMillis();
      count.set(0);
    }
    table.close();
    dom.close();
  }

  static class Worker extends Thread {
    final Transaction t;
    final HTableInterface table;

    Worker(Transaction t, HTableInterface table) {
      this.table = table;
      this.t = t;
    }

    public void run() {
      byte[] cf = "test-cf1".getBytes();
      byte[] col = "test-col1".getBytes();
      int i = 0;
      while (true) {
        Put put = new Put(String.format("KEY_%05d", i).getBytes());
        put.add(cf, col, String.format("hello_%05d", i).getBytes());
        try {
          t.put(put, table);
          count.addAndGet(1);
        }
        catch (IOException e) {
          e.printStackTrace();
          break;
        }
        i = (i + 1) % 100000;
      }
    }
  }

}

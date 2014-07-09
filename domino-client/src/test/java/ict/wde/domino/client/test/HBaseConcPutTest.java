package ict.wde.domino.client.test;

import ict.wde.domino.console.Env;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

public class HBaseConcPutTest {

  static final AtomicLong count = new AtomicLong();
  static Configuration conf;

  public static void main(String[] args) throws Exception {

    String hbaseConf = Env.HBASE_SITE;
    if (args.length > 0) hbaseConf = args[0];

    conf = new Configuration();
    conf.addResource(new Path(hbaseConf));
    Worker worker[] = new Worker[100];
    for (int i = 0; i < worker.length; ++i) {
      worker[i] = new Worker();
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
  }

  static class Worker extends Thread {
    final HTableInterface table;

    Worker() throws IOException {
      table = new HTable(conf, "dom-test");
    }

    public void run() {
      byte[] cf = "test-cf1".getBytes();
      byte[] col = "test-col1".getBytes();
      Random rand = new Random();
      while (true) {
        try {
          int total = rand.nextInt(50) + 20;
          for (int i = 0; i < total; ++i) {
            byte[] key = ConcPutTest.genKey(rand);
            // Get get = new Get(key);
            // get.addFamily(DominoConst.INNER_FAMILY);
            // table.get(get);
            Put put = new Put(key);
            put.add(cf, col, ConcPutTest.value);
            table.put(put);
            count.addAndGet(1);
          }
        }
        catch (IOException e) {
          System.err.println(e);
        }
      }
    }
  }

}

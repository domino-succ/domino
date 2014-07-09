package ict.wde.domino.client.test;

import ict.wde.domino.client.Domino;
import ict.wde.domino.client.Transaction;
import ict.wde.domino.console.Env;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class TrxTest {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(new Path(Env.HBASE_SITE));
    HTable table = new HTable(conf, "dom-test");
    Domino dom = new Domino(conf);
    Transaction trx = dom.startTransaction();
    // for (int i = 0; i < 10; ++i) {
    // Put put = new Put(("IamKEY_" + i).getBytes());
    // put.add(cf, col, "aaaa".getBytes());
    // trx.put(put, table);
    // }
    Result r = trx.get(new Get("IamKEY_0".getBytes()), table);
    System.out.println(r.isEmpty());
    DUtils.printResult(r);
    System.out.println("Sleeping for checking...");
    Thread.sleep(10000);
    trx.rollback();
    dom.close();
    table.close();
  }
}

package ict.wde.domino.client.test;

import ict.wde.domino.client.Domino;
import ict.wde.domino.client.Transaction;
import ict.wde.domino.console.Env;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class ScanTest {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(new Path(Env.HBASE_SITE));
    HTable table = new HTable(conf, "WAREHOUSE");
    Domino dom = new Domino(conf);
    Transaction trx = dom.startTransaction();
    Iterator<Result> res = trx.scan(new Scan(), table).iterator();
    while (res.hasNext()) {
      DUtils.printResult(res.next());
    }
    trx.commit();
    dom.close();
    table.close();
  }
}

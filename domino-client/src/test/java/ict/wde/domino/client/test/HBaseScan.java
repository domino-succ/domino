package ict.wde.domino.client.test;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseScan {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", "p18:2181");

    HTable table = new HTable(conf, "WAREHOUSE".getBytes());
    // Get get = new Get("0000142125".getBytes());
    // get.setMaxVersions();
    Scan scan = new Scan();
    scan.setMaxVersions(1000);
    ResultScanner rs = table.getScanner(scan);
    Iterator<Result> it = rs.iterator();
    while (it.hasNext()) {
      Result r = it.next();
      // Result r = table.get(get);
      DUtils.printResult(r);
    }
    rs.close();
    table.close();
  }
}

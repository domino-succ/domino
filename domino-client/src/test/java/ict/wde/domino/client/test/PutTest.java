package ict.wde.domino.client.test;

import ict.wde.domino.common.DominoConst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class PutTest {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set(DominoConst.ZK_PROP, "processor145:2181");
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.majorCompact("t");
    admin.close();
    HTable t = new HTable(conf, "t");
    int base = 0;
    int count = 1;
    for (int i = base; i < base + count; ++i) {
      Put p1 = new Put("008".getBytes());
      p1.add("c1".getBytes(), "c".getBytes(), i, ("v" + i).getBytes());
      t.put(p1);
    }
    t.close();
    // Domino dom = new Domino(conf);
    // Transaction trx = dom.startTransaction();
    // Put put = new Put("I am key 2".getBytes());
    // put.add("test-cf1".getBytes(), "test-col1".getBytes(),
    // "aaaa".getBytes());
    // trx.put(put, new HTable(conf, "dom-test"));
    // dom.close();
  }
}

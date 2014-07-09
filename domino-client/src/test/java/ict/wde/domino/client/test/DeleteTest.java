package ict.wde.domino.client.test;

import ict.wde.domino.common.DominoConst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;

public class DeleteTest {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set(DominoConst.ZK_PROP, "processor145:2181");
    HTable t = new HTable(conf, "t");
    Delete d = new Delete("008".getBytes());
    d.deleteColumn("c1".getBytes(), "c".getBytes(), 2);
    t.delete(d);
    t.close();
  }
}

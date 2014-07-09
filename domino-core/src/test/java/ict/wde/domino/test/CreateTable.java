package ict.wde.domino.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateTable {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(new Path(Env.HBASE_SITE));

    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      admin.disableTable(Env.TABLE);
    }
    catch (Exception e) {
    }
    try {
      admin.deleteTable(Env.TABLE);
    }
    catch (Exception e) {
    }
    HTableDescriptor tabd = new HTableDescriptor(Env.TABLE);
    tabd.addCoprocessor("ict.wde.domino.cop.DominoEndpoint");
    for (int i = 0; i < Env.CF.length; ++i) {
      HColumnDescriptor cf = new HColumnDescriptor(Env.CF[i]);
      tabd.addFamily(cf);
    }
    admin.createTable(tabd, Env.SPLB);
    admin.close();
  }
}

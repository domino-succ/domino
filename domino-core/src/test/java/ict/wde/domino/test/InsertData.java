package ict.wde.domino.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class InsertData {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(new Path(Env.HBASE_SITE));

    HTable table = new HTable(conf, Env.TABLE);
    byte[] cf = Env.CF[0].getBytes();
    byte[] col = Env.COL[0][0].getBytes();
    for (int i = 0; i <= 7; ++i) {
      for (int j = 0; j < 10; ++j) {
        String rk = String.format("%d%05d", i, j);
        Put put = new Put(rk.getBytes());
        put.add(cf, col, ("some value - " + rk).getBytes());
        table.put(put);
      }
    }
    table.close();
  }
}

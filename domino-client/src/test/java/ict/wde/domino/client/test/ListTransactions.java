package ict.wde.domino.client.test;

import ict.wde.domino.common.DominoConst;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class ListTransactions {

  public static void main(String[] args) throws IOException {

    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", "p18:2181");

    HTable table = new HTable(conf, DominoConst.TRANSACTION_META);
    Iterator<Result> rs = table.getScanner(new Scan()).iterator();
    while (rs.hasNext()) {
      DUtils.printTransaction(rs.next());
    }
    table.close();
  }

}

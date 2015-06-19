package ict.wde.domino.id;

import ict.wde.domino.common.DominoConst;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Tieying Zhang, ICT, CAS
 *
 */

public class TidTest {
  public static void main(String[] args) throws Throwable {
    // System.setProperty(TidDef.PROP_STANDALONE, "true");
    Configuration conf = new Configuration();
    conf.set(DominoConst.ZK_PROP, "ccf104:2181");
    DominoIdIface client = DominoIdService.getClient(conf);
    for (int i = 0; i < 100; ++i) {
      long testId = client.getId();
      System.out.println(testId);
      Thread.sleep(1000);
    }
    client.close();
  }
}

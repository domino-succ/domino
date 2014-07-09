package ict.wde.domino.id;

public class TidTest {
  public static void main(String[] args) throws Throwable {
    // System.setProperty(TidDef.PROP_STANDALONE, "true");
    DominoIdIface client = DominoIdService.getClient("p145:2181");
    for (int i = 0; i < 100; ++i) {
      System.out.println(client.getId());
      Thread.sleep(1000);
    }
    client.close();
  }
}

/**
 *  Copyright 2014 ZhenZhao and Tieying Zhang. 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package ict.wde.domino.console;

import ict.wde.domino.client.Domino;
import ict.wde.domino.client.Transaction;
import ict.wde.domino.common.DominoConst;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import jline.console.ConsoleReader;

import org.apache.hadoop.conf.Configuration;

public class DominoConsole {

  static final String DOMINO_PROMPT = "domino> ";

  static Configuration config = new Configuration();
  static Domino domino;
  static ConsoleReader reader;
  static AtomicReference<Transaction> trx = new AtomicReference<>(null);

  public static void main(String[] args) throws Throwable {
    config.set(DominoConst.ZK_PROP, args[0]);
    domino = new Domino(config);
    initReader();
    Runtime.getRuntime().addShutdownHook(hook);
    printGPLHeader();
    while (true) {
      String line = reader.readLine(DOMINO_PROMPT).trim();
      if (line.length() == 0) continue;
      String cmd = getFirstToken(line);
      try {
        Command op = getOperation(cmd, line, trx);
        if (op != null) {
          String[][] res = op.execute();
          if (res != null) {
            System.out.println(TableDrawer.draw(res));
          }
          System.out.println(op.numRowsInvolved() + " row(s) read/written.");
          continue;
        }
        else if ("commit".equalsIgnoreCase(cmd)) {
          if (trx.get() == null) {
            System.out.println("No transaction to commit.");
            continue;
          }
          try {
            trx.get().commit();
            System.out.println("Commit complete.");
            trx.set(null);
          }
          catch (IOException ioe) {
            System.out.println("Commit Failed: " + ioe.toString());
            System.out.println("Try to restart the transaction. ");
          }
        }
        else if ("rollback".equalsIgnoreCase(cmd)) {
          if (trx.get() == null) {
            System.out.println("No transaction to rollback.");
            continue;
          }
          trx.get().rollback();
          System.out.println("Rollback complete.");
          trx.set(null);
        }
        else if ("quit".equalsIgnoreCase(cmd)) {
          break;
        }
        else {
          printUsage();
        }
      }
      catch (Throwable t) {
        System.out.println("Got an error: " + t.toString());
        t.printStackTrace(System.out);
      }
    }
  }

  static void printGPLHeader() {
    System.out.println("Domino Console Alpha  Copyright (C) 2014  Zhen Zhao");
    System.out
        .println("This program comes with ABSOLUTELY NO WARRANTY; for details type `show w'.");
    System.out
        .println("This is free software, and you are welcome to redistribute it");
    System.out.println("under certain conditions; type `show c' for details.");
  }

  static void printUsage() {
    System.out.println("Usage: ");
    System.out.println("SELECT <columns> FROM <table> [WHERE <condition>];");
    System.out
        .println("[RW] UPSERT <table> VALUES <column>=<value>,... WHERE row=<row_key>;");
    System.out.println("DELETE FROM <table> WHERE row=<row_key>;");
    System.out.println("CREATE TABLE <table> <column_family>,...;");
    System.out.println("DROP TABLE <table>;");
    System.out.println("ROLLBACK");
    System.out.println("COMMIT");
    System.out.println("QUIT");
  }

  static final Thread hook = new Thread() {
    @Override
    public void run() {
      if (domino == null) {
        return;
      }
      System.out.println("Exiting console...");
      if (trx.get() != null) {
        System.out.println("Committing the rest operations...");
        try {
          trx.get().commit();
        }
        catch (IOException ioe) {
          System.out.println("Failed to commit, aborting...");
          try {
            trx.get().rollback();
          }
          catch (IOException e) {
          }
        }
      }
      try {
        domino.close();
      }
      catch (IOException e) {
      }
    }
  };

  static Command getOperation(String cmd, String sql,
      AtomicReference<Transaction> trx) throws IOException {
    if ("select".equalsIgnoreCase(cmd)) {
      if (trx.get() == null) {
        trx.set(domino.startTransaction());
      }
      return new CmdSelect(sql, domino, trx.get());
    }
    else if ("upsert".equalsIgnoreCase(cmd)) {
      if (trx.get() == null) {
        trx.set(domino.startTransaction());
      }
      return new CmdUpsert(sql, domino, trx.get(), false);
    }
    else if ("rw".equalsIgnoreCase(cmd)) {
      if (trx.get() == null) {
        trx.set(domino.startTransaction());
      }
      return new CmdUpsert(sql.substring(3), domino, trx.get(), true);
    }
    else if ("delete".equalsIgnoreCase(cmd)) {
      if (trx.get() == null) {
        trx.set(domino.startTransaction());
      }
      return new CmdDelete(sql, domino, trx.get());
    }
    else if ("create".equalsIgnoreCase(cmd)) {
      return new CmdCreate(sql, domino, trx.get());
    }
    else if ("drop".equalsIgnoreCase(cmd)) {
      return new CmdDrop(sql, domino, trx.get());
    }
    return null;
  }

  static String getFirstToken(String s) {
    int i = 0;
    while (i < s.length() && Character.isSpaceChar(s.charAt(i))) {
      ++i;
    }
    while (i < s.length() && Character.isAlphabetic(s.charAt(i))) {
      ++i;
    }
    return s.substring(0, i);
  }

  static void initReader() throws IOException {
    reader = new ConsoleReader();
    reader.setBellEnabled(false);
    reader.setAutoprintThreshold(10);
    reader.setHistoryEnabled(true);
  }

}

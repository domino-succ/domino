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

public class Env {
  public static final String TABLE = "t";
  public static final String[] CF = { "cf1" };
  public static final String[][] COL = { { "col1", "col2" } };
  public static final String[] SPL = { "1", "2", "3", "4", "5", "6" };
  public static final byte[][] SPLB;
  static {
    SPLB = new byte[SPL.length][];
    for (int i = 0; i < SPL.length; ++i) {
      SPLB[i] = SPL[i].getBytes();
    }
  }
}

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

package ict.wde.domino.id;

import java.io.IOException;

import ict.wde.domino.common.DominoConst;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * DTO Constants definition.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class TidDef {

  public static final long PRE_ALLOC_BATCH = 65536;

  public static final long START_ID = 2;

  public static final String ZK_ROOT_PATH = "/domino";
  public static final String ZK_LOCK_PATH = "/master";
  public static final String ZK_TID_PATH = "/tid";

  public static final byte[] EP_ROW = "0".getBytes();
  public static final byte[] EP_FAMILY = "_tis".getBytes();
  public static final byte[] EP_COLUMN = "_id".getBytes();
  public static final long EP_VERSION = 1;

  public static final HTableDescriptor EP_TABLE_DESCRIPTOR;

  static {
    EP_TABLE_DESCRIPTOR = new HTableDescriptor(DominoConst.TID_EP_TABLE);
    HColumnDescriptor family = new HColumnDescriptor(EP_FAMILY);
    family.setInMemory(true);
    family.setMaxVersions(1);
    EP_TABLE_DESCRIPTOR.addFamily(family);
    try {
      EP_TABLE_DESCRIPTOR.addCoprocessor("ict.wde.domino.id.cop.TidEPServer");
    }
    catch (IOException ioe) {
    }
  }

}

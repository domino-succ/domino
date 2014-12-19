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

package ict.wde.domino.id.cop;

import ict.wde.domino.id.DominoIdIface;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * DTO Endpoint Interface.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public interface TidEPIface extends Coprocessor, CoprocessorProtocol,
    DominoIdIface {
}

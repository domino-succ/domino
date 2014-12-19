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

 package ict.wde.domino.common;
 
 import org.apache.hadoop.conf.Configuration;
 
 /**
 * Domino ZK configuration helper
 * 
 * @author Chenbo, ICT, CAS
 * 
 */
 public class ConfigurationHelper{
		public static Configuration config(String zookeeperAddress){
			String[] hosts = zookeeperAddress.split(",");
			String quarom = "";
			int port = 2181;//default
			for(String h: hosts){
				String host = h.trim();
				if(host.isEmpty()) continue;
				if(!quarom.isEmpty()) quarom += ",";
				int i = host.indexOf(':');
				if(i>0){
					if(quarom.isEmpty()){//first port
						port = Integer.parseInt(host.substring(i+1));
					}
					quarom += host.substring(0, i);
				}else{
					quarom += host;
				}
			}
			if(quarom.isEmpty()){
				throw new RuntimeException("Invalid argument!");
			}
			return config(quarom, port);
		}
		
		public static Configuration config(String quarom, int port){
			Configuration cfg = new Configuration();
			cfg.set(DominoConst.ZK_PROP, quarom);
			cfg.set(DominoConst.ZK_CLIENT_PORT_PROP, String.valueOf(port));
			return cfg;
		}
 }
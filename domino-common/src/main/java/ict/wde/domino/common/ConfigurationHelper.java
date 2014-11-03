/**
 *  Domino, A Transaction Engine Based on Apache HBase
 *  Copyright (C) 2014  Zhen Zhao
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
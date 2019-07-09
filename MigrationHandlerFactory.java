package com.infa.pc.migration.utils;

public class MigrationHandlerFactory {

	private static MigrationHandler handler;
	
	public static MigrationHandler getMigrationHandler(String connectionType) 
	{ 
		if ( connectionType.equals("SnowflakeCloudDataWarehouse") ){
			if(handler == null || !(handler instanceof SnowflakeMigrationHandler)){
				handler = new SnowflakeMigrationHandler();
			}
			
		}
		
		return handler;
	}
}

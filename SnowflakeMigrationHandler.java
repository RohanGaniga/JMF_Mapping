package com.infa.pc.migration.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;

public class SnowflakeMigrationHandler extends MigrationHandler{

	private Properties runtimeProps;

	@Override
	protected void updateTypes(Field field) {
		switch(field.getDataType()){
		case "integer":
		case "byteint":
		case "smallint":
		case "small integer":
		case "bigint" : 
			field.setPrecision("38");
			field.setScale("0");
			field.setDataType("NUMBER");
			break;
		case "date": 	
			field.setPrecision("38");
			field.setScale("10");
			field.setDataType("DATE");
			break;
		case "date/time":
		case "time":
		case "timestamp": 	field.setPrecision("38");
		field.setScale("9");
		field.setDataType("TIMESTAMPNTZ");
		break;
		case "char":
		case "string":
		case "nstring":
		case "ntext":
		case "byte":
		case "text":
		case "varbyte":
		case "varchar": 
			field.setDataType("VARCHAR");
			break;
		case "decimal":
			/*field.setDataType("DECIMAL");
							break;*/
		case "float": 
		case "double":
		case "real":
			field.setDataType( "DOUBLE"); 
			field.setPrecision("52");
			field.setScale("5");
			break;
		case "binary" : 
			field.setDataType("BINARY");
			break;
		default:		field.setDataType("OBJECT");
		}

		if(field.getFieldType() == FieldType.TRANSFORM){
			field.setFieldType(FieldType.SOURCE);
		}
	}

	@Override
	protected void updateFieldAttributeValues(Field field) {
		//TODO : Updated field attributes with user specified values
	}

	@Override
	protected String getDomainName() {
		return "SnowflakeCloudDataWarehouse DOMAIN";
	}

	@Override
	protected String getAdapterID() {
		return "com.infa.adapter.snowflake";
	}

	@Override
	protected HashMap<String, Object> getConnectionInfo() {
		HashMap<String, Object> connInfo = new HashMap<>();
		connInfo.put("password", "unsafe94063AAAA");
		connInfo.put("role", "");
		connInfo.put("additionalparam", "AAA");
		connInfo.put("warehouse", "TEST_WH");
		connInfo.put("user", "test_infAAA");
		connInfo.put("account", "informaticaAAA");
		
		return connInfo;
	}
}

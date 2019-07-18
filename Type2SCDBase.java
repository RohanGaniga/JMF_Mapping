/*
 * Base.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldKeyType;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.INameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NativeDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.SASHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.StringConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.RepoOperationException;
import com.informatica.powercenter.sdk.mapfwk.repository.RepoPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;

/**
 *
 *
 */
public abstract class Type2SCDBase {
	// ///////////////////////////////////////////////////////////////////////////////////
	// Instance variables
	// ///////////////////////////////////////////////////////////////////////////////////
	protected Repository rep;
	protected Folder folder;
	protected Session session;
	protected Workflow workflow;
	protected String mapFileName;
	protected Mapping mapping;
	protected int runMode = 0;
	//private Field dept_idField;
	//private Field SalaryField;

	/**
	 * Common execute method
	 */
	public void execute() throws Exception {
		init();
		createMappings();
		createSession();
		createWorkflow();
		generateOutput();

	}

	/**
	 * Initialize the method
	 */
	protected void init() {
		createRepository();
		createFolder();
		createSources();
		createTargets();
	}

	/**
	 * Create a repository
	 */
	protected void createRepository() {
		rep = new Repository("PowerCenter", "PowerCenter",
				"This repository contains API test samples");
	}

	/**
	 * Creates a folder
	 */
	protected void createFolder() {
		folder = new Folder("JavaMappingSamples", "JavaMappingSamples",
				"This is a folder containing java mapping samples");
		rep.addFolder(folder);
	}

	/**
	 * Create sources
	 */
	protected abstract void createSources(); // override in base class to create
												// appropriate sources

	/**
	 * Create targets
	 */
	protected abstract void createTargets(); // override in base class to create
												// appropriate targets

	/**
	 * Creates a mapping It needs to be overriddden for the sample
	 * 
	 * @return Mapping
	 */
	protected abstract void createMappings() throws Exception; // override in
																// base class

	/**
	 * Create session
	 */
	protected abstract void createSession() throws Exception;

	/**
	 * Create workflow
	 */
	protected abstract void createWorkflow() throws Exception;

	/**
	 * Create source for Employee Source
	 */
	protected Source createEmployeeSource() {
		List<Field> fields = new ArrayList<Field>();
		Field field1 = new Field("EmployeeID", "EmployeeID", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(field1);

		Field field2 = new Field("LastName", "LastName", "",
				NativeDataTypes.FlatFile.STRING, "20", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field2);

		Field field3 = new Field("FirstName", "FirstName", "",
				NativeDataTypes.FlatFile.STRING, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field3);

		Field field4 = new Field("Title", "Title", "",
				NativeDataTypes.FlatFile.STRING, "30", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field4);

		Field field5 = new Field("TitleOfCourtesy", "TitleOfCourtesy", "",
				NativeDataTypes.FlatFile.STRING, "25", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field5);

		Field field6 = new Field("BirthDate", "BirthDate", "",
				NativeDataTypes.FlatFile.DATETIME, "19", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field6);

		Field field7 = new Field("HireDate", "HireDate", "",
				NativeDataTypes.FlatFile.DATETIME, "19", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field7);

		Field field8 = new Field("Address", "Address", "",
				NativeDataTypes.FlatFile.STRING, "60", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field8);

		Field field9 = new Field("City", "City", "",
				NativeDataTypes.FlatFile.STRING, "15", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field9);

		Field field10 = new Field("Region", "Region", "",
				NativeDataTypes.FlatFile.STRING, "15", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field10);

		Field field11 = new Field("PostalCode", "PostalCode", "",
				NativeDataTypes.FlatFile.STRING, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field11);

		Field field12 = new Field("Country", "Country", "",
				NativeDataTypes.FlatFile.STRING, "15", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field12);

		Field field13 = new Field("HomePhone", "HomePhone", "",
				NativeDataTypes.FlatFile.STRING, "24", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field13);

		Field field14 = new Field("Extension", "Extension", "",
				NativeDataTypes.FlatFile.STRING, "4", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field14);

		Field field15 = new Field("Notes", "Notes", "",
				NativeDataTypes.FlatFile.STRING, "350", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field15);

		Field field16 = new Field("ReportsTo", "ReportsTo", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field16);

		ConnectionInfo info = getFlatFileConnectionInfo();
		info.getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_FILENAME, "Employees.csv");
		Source employeeSource = new Source("Employee", "Employee",
				"This is Employee Table", "Employee", info);
		employeeSource.setFields(fields);
		return employeeSource;
	}

	protected Source createNetezzaSource() {
		List<Field> fields = new ArrayList<Field>();
		Field field1 = new Field("EmployeeID", "EmployeeID", "",
				NativeDataTypes.Netezza.CHAR, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(field1);

		Field field2 = new Field("LastName", "LastName", "",
				NativeDataTypes.Netezza.CHAR, "20", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field2);

		Field field3 = new Field("FirstName", "FirstName", "",
				NativeDataTypes.Netezza.CHAR, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field3);

		Field field4 = new Field("Title", "Title", "",
				NativeDataTypes.Netezza.CHAR, "30", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field4);

		ConnectionInfo info = getNetezzaConnectionInfo();

		Source employeeSource = new Source("Netezza_Employee",
				"Netezza_Employee", "This is Employee Table",
				"Netezza_Employee", info);
		employeeSource.setFields(fields);
		return employeeSource;
	}

	/**
	 * Create source for Company
	 */
	protected Source createMaraSource() {
		List<Field> fields = new ArrayList<Field>();
		Field field1 = new Field("MANDT", "MANDT", "MANDT",
				NativeDataTypes.FlatFile.STRING, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field1);
		Field field2 = new Field("MATNR", "MATNR", "MATNR",
				NativeDataTypes.FlatFile.STRING, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field2);
		Field field3 = new Field("BUKRS", "BUKRS", "BUKRS",
				NativeDataTypes.FlatFile.STRING, "20", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field3);

		Field field4 = new Field("MAKTX", "MAKTX", "MAKTX",
				NativeDataTypes.FlatFile.STRING, "20", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field4);
		ConnectionInfo info = getFlatFileConnectionInfo();/*
														 * info.getConnProps().
														 * setProperty
														 * (ConnectionPropsConstants
														 * .SOURCE_FILENAME,
														 * "Employees.csv");
														 */
		Source mara = new Source("MARA", "MARA", "MARA", "MARA", info);
		mara.setFields(fields);
		return mara;
	}

	/**
	 * Create source for Employee Source
	 */
	protected Source createOrderDetailSource() {
		List<Field> fields = new ArrayList<Field>();
		Field field1 = new Field("OrderID", "OrderID", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.FOREIGN_KEY, FieldType.SOURCE, false);
		fields.add(field1);

		Field field2 = new Field("ProductID", "ProductID", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.FOREIGN_KEY, FieldType.SOURCE, false);
		fields.add(field2);

		Field field3 = new Field("UnitPrice", "UnitPrice", "",
				NativeDataTypes.FlatFile.NUMBER, "28", "4",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field3);

		Field field4 = new Field("Quantity", "Quantity", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field4);

		Field field5 = new Field("Discount", "Discount", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field5);

		Field field6 = new Field("StringFld", "StringFld", "",
				NativeDataTypes.FlatFile.STRING, "5", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field6);

		Field field7 = new Field("String2Fld", "String2Fld", "",
				NativeDataTypes.FlatFile.STRING, "5", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field7);

		ConnectionInfo info = getFlatFileConnectionInfo();
		info.getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_FILENAME, "Order_Details.csv");
		Source ordDetailSource = new Source("OrderDetail", "OrderDetail",
				"This is Order Detail Table", "OrderDetail", info);
		ordDetailSource.setFields(fields);
		return ordDetailSource;
	}

	/**
	 * Create source for Items table
	 */
	protected Source createItemsSource() {
		Source itemsSource;
		List<Field> fields = new ArrayList<Field>();
		Field field1 = new Field("ItemId", "ItemId", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(field1);

		Field field2 = new Field("Item_Name", "Item_Name", "",
				NativeDataTypes.FlatFile.STRING, "72", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field2);

		Field field3 = new Field("Item_Desc", "Item_Desc", "",
				NativeDataTypes.FlatFile.STRING, "72", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field3);

		Field field4 = new Field("Price", "Price", "",
				NativeDataTypes.FlatFile.NUMBER, "10", "2",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field4);

		Field field5 = new Field("Wholesale_cost", "Wholesale_cost", "",
				NativeDataTypes.FlatFile.NUMBER, "10", "2",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field5);

		Field field6 = new Field("Manufacturer_id", "Manufacturer_id", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field6);

		ConnectionInfo info = getFlatFileConnectionInfo();
		info.getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_FILENAME, "items.csv");
		itemsSource = new Source("Items", "Items", "This is Items table",
				"Items", info);
		itemsSource.setFields(fields);
		return itemsSource;
	}

	/**
	 * Create source for Products table
	 */
	protected Source createProductsSource() {
		Source productSource;
		List<Field> fields = new ArrayList<Field>();

		Field foriegnfield = new Field("ItemId", "ItemId", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.FOREIGN_KEY, FieldType.SOURCE, false);
		foriegnfield.setReferenceConstraint("Items", "ItemId");
		fields.add(foriegnfield);

		Field field1 = new Field("Item_No", "Item_No", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(field1);

		Field field2 = new Field("Item_Name", "Item_Name", "",
				NativeDataTypes.FlatFile.STRING, "72", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field2);

		Field field3 = new Field("Item_Desc", "Item_Desc", "",
				NativeDataTypes.FlatFile.STRING, "72", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field3);

		Field field4 = new Field("Cust_Price", "Cust_Price", "",
				NativeDataTypes.FlatFile.NUMBER, "10", "2",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field4);

		Field field5 = new Field("Product_Category", "Product_Category", "",
				NativeDataTypes.FlatFile.STRING, "30", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field5);

		ConnectionInfo info = getFlatFileConnectionInfo();
		info.getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_FILENAME, "products.csv");
		productSource = new Source("Products", "Products",
				"This is products table", "Products", info);
		productSource.setFields(fields);
		return productSource;
	}

	/**
	 * This method creates the source for manufacturers table
	 * 
	 * @return Source object
	 */
	protected Source createManufacturersSource() {
		Source manufacturerSource;
		List<Field> fields = new ArrayList<Field>();
		Field field1 = new Field("Manufacturer_Id", "Manufacturer_Id", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(field1);

		Field field2 = new Field("Manufacturer_Name", "Manufacturer_Name", "",
				NativeDataTypes.FlatFile.STRING, "72", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field2);

		ConnectionInfo info = getFlatFileConnectionInfo();
		info.getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_FILENAME, "Manufacturer.csv");
		manufacturerSource = new Source("Manufacturers", "Manufacturers",
				"This is Manufacturers table", "Manufacturers", info);
		manufacturerSource.setFields(fields);
		return manufacturerSource;
	}
	
	protected Source createLkpEmployeeIdSource(String dbName) {
		Source manufacturerSource = null;

		List<Field> fields = new ArrayList<Field>();
		Field cust_keyField = new Field("cust_key", "cust_key", "",
				NativeDataTypes.SqlServer.INT, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(cust_keyField);                                                                        

		Field Customer_idField = new Field("Customer_id", "Customer_id", "",
				NativeDataTypes.SqlServer.INT, "10", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(Customer_idField);

		Field LocationField = new Field("Location", "Location", "",
				NativeDataTypes.SqlServer.VARCHAR, "20", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(LocationField);
		
		Field flagField = new Field("flag", "flag", "",
				NativeDataTypes.SqlServer.INT, "20", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(flagField);
		
		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.Microsoft_SQL_Server,
				dbName);
		manufacturerSource = new Source("customers_Dim3", "Customers_Dim3", "This is Customers_Dim3 table", "Customers_Dim3",
				info);
		manufacturerSource.setFields(fields);
		return manufacturerSource;
	}

	/**
	 * Create Orders Source
	 * 
	 * @return
	 */
	protected Source createOrdersSource() {
		Source ordersSource;
		List<Field> fields = new ArrayList<Field>();
		Field field1 = new Field("OrderID", "OrderID", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(field1);

		Field field2 = new Field("CustomerID", "Customer", "",
				NativeDataTypes.FlatFile.STRING, "5", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field2);

		Field field3 = new Field("EmployeeID", "EmployeeID", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.FOREIGN_KEY, FieldType.SOURCE, false);
		fields.add(field3);

		Field field5 = new Field("OrderDate", "OrderDate", "",
				NativeDataTypes.FlatFile.DATETIME, "19", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field5);

		Field field6 = new Field("RequiredDate", "RequiredDate", "",
				NativeDataTypes.FlatFile.DATETIME, "19", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field6);

		Field field7 = new Field("ShippedDate", "ShippedDate", "",
				NativeDataTypes.FlatFile.DATETIME, "19", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field7);

		Field field8 = new Field("ShipVia", "ShipVia", "",
				NativeDataTypes.FlatFile.INT, "10", "0",
				FieldKeyType.FOREIGN_KEY, FieldType.SOURCE, false);
		fields.add(field8);

		Field field9 = new Field("Freight", "Freight", "",
				NativeDataTypes.FlatFile.NUMBER, "28", "4",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field9);

		Field field10 = new Field("ShipName", "ShipName", "",
				NativeDataTypes.FlatFile.STRING, "40", "",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field10);

		Field field11 = new Field("ShipAddress", "ShipAddress", "",
				NativeDataTypes.FlatFile.STRING, "60", "",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field11);

		Field field12 = new Field("ShipCity", "ShipCity", "",
				NativeDataTypes.FlatFile.STRING, "15", "",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field12);

		Field field13 = new Field("ShipRegion", "ShipRegion", "",
				NativeDataTypes.FlatFile.STRING, "15", "",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field13);

		Field field14 = new Field("ShipPostalCode", "ShipPostalCode", "",
				NativeDataTypes.FlatFile.STRING, "10", "",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field14);

		Field field15 = new Field("ShipCountry", "ShipCountry", "",
				NativeDataTypes.FlatFile.STRING, "15", "",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(field15);

		ConnectionInfo info = getFlatFileConnectionInfo();
		info.getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_FILENAME, "Orders.csv");
		ordersSource = new Source("Orders", "Orders", "This is Orders table",
				"Orders", info);
		ordersSource.setFields(fields);
		return ordersSource;
	}

	/**
	 * Method to create Job Info relational source for Oracle database
	 * 
	 * @param dbName
	 *            database name
	 */
	protected Source createOracleJobSource(String dbName) {
		Source jobSource = null;

		List<Field> fields = new ArrayList<Field>();
		Field CustomerField_Id = new Field("Customer_Id", "Customer_Id", "",
				NativeDataTypes.SqlServer.INT, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(CustomerField_Id);                                                                        

	
		Field LocationField = new Field("Location", "Location", "",
				NativeDataTypes.SqlServer.VARCHAR, "20", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(LocationField);
		
		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.Microsoft_SQL_Server,
				dbName);
		jobSource = new Source("cust", "cust", "This is cust table", "cust",
				info);
		jobSource.setFields(fields);
		return jobSource;
	}
		
		
		
		/*Field firstnameField = new Field("firstName", "firstName", "",
				NativeDataTypes.SqlServer.INT, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(firstnameField);
		
		Field lastnameField = new Field("lastname", "lastname", "",
				NativeDataTypes.SqlServer.INT, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(lastnameField);*/
		
		/*Field secondnameField = new Field("firstname", "firstname", "",
				NativeDataTypes.SqlServer.INT, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(secondnameField);*/

		/*ConnectionInfo info = getRelationalConnInfo(SourceTargetType.Microsoft_SQL_Server,
				dbName);
		jobSource = new Source("JOBS", "JOBS", "This is JOBS table", "JOBS",
				info);
		jobSource.setFields(fields);
		return jobSource;
	}*/

	protected Source createSASSource(String dbName) {
		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.SAS,
				dbName);
		info.getConnProps().setProperty(
				ConnectionPropsConstants.CONNECTIONNAME, "SAS");
		Source sasSrc = new Source("DATATYPES_IN", "DATATYPES_IN",
				"DATATYPES_IN", "DATATYPES_IN", info);

		List<Field> fields = new ArrayList<Field>();
		Field ind = SASHelper.configureField(new Field("ind", "ind", "ind",
				NativeDataTypes.SAS.NUMBER, "5", "0", FieldKeyType.NOT_A_KEY,
				FieldType.SOURCE, false), "5.");
		fields.add(ind);
		Field fldShortText = SASHelper.configureField(new Field("fldShortText",
				"fldShortText", "fldShortText", NativeDataTypes.SAS.STRING,
				"25", "0", FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false),
				"$25.");
		fields.add(fldShortText);
		Field fldLongText = SASHelper.configureField(new Field("fldLongText",
				"fldLongText", "fldLongText", NativeDataTypes.SAS.STRING, "90",
				"0", FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false), "$90.");
		fields.add(fldLongText);
		Field fldNumeric7_2 = SASHelper.configureField(new Field(
				"fldNumeric7_2", "fldNumeric7_2", "fldNumeric7_2",
				NativeDataTypes.SAS.NUMBER, "6", "2", FieldKeyType.NOT_A_KEY,
				FieldType.SOURCE, false), "7.2");
		fields.add(fldNumeric7_2);
		Field fldNumeric15 = SASHelper.configureField(new Field("fldNumeric15",
				"fldNumeric15", "fldNumeric15", NativeDataTypes.SAS.NUMBER,
				"15", "0", FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false),
				"15.");
		fields.add(fldNumeric15);
		Field fldDate = SASHelper.configureField(new Field("fldDate",
				"fldDate", "fldDate", NativeDataTypes.SAS.DATE, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false), "DATE5.");
		fields.add(fldDate);
		Field fldDateTime = SASHelper.configureField(new Field("fldDateTime",
				"fldDateTime", "fldDateTime", NativeDataTypes.SAS.DATETIME,
				"12", "0", FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false),
				"DATETIME24.");
		fields.add(fldDateTime);
		Field fldTime = SASHelper.configureField(new Field("fldTime",
				"fldTime", "fldTime", NativeDataTypes.SAS.TIME, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false), "TIME.");
		fields.add(fldTime);
		Field fldYYYMMDD = SASHelper.configureField(new Field("fldYYYMMDD",
				"fldYYYMMDD", "fldYYYMMDD", NativeDataTypes.SAS.DATE, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false), "YYMMDD.");
		fields.add(fldYYYMMDD);

		sasSrc.setFields(fields);

		SASHelper.configureSASSource(sasSrc, "TDATASET", "", "", "");
		return sasSrc;
	}

	protected Target createSASTarget(String dbName, String name) {
		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.SAS,
				dbName);

		Target sasTgt = new Target(name, name, name, name, info);
		List<Field> fields = new ArrayList<Field>();
		Field ind = SASHelper.configureField(new Field("ind", "ind", "ind",
				NativeDataTypes.SAS.NUMBER, "5", "0", FieldKeyType.NOT_A_KEY,
				FieldType.TARGET, false), "5.");
		fields.add(ind);
		Field fldShortText = SASHelper.configureField(new Field("fldShortText",
				"fldShortText", "fldShortText", NativeDataTypes.SAS.STRING,
				"25", "0", FieldKeyType.NOT_A_KEY, FieldType.TARGET, false),
				"$25.");
		fields.add(fldShortText);
		Field fldLongText = SASHelper.configureField(new Field("fldLongText",
				"fldLongText", "fldLongText", NativeDataTypes.SAS.STRING, "90",
				"0", FieldKeyType.NOT_A_KEY, FieldType.TARGET, false), "$90.");
		fields.add(fldLongText);
		Field fldNumeric7_2 = SASHelper.configureField(new Field(
				"fldNumeric7_2", "fldNumeric7_2", "fldNumeric7_2",
				NativeDataTypes.SAS.NUMBER, "6", "2", FieldKeyType.NOT_A_KEY,
				FieldType.TARGET, false), "7.2");
		fields.add(fldNumeric7_2);
		Field fldNumeric15 = SASHelper.configureField(new Field("fldNumeric15",
				"fldNumeric15", "fldNumeric15", NativeDataTypes.SAS.NUMBER,
				"15", "0", FieldKeyType.NOT_A_KEY, FieldType.TARGET, false),
				"15.");
		fields.add(fldNumeric15);
		Field fldDate = SASHelper.configureField(new Field("fldDate",
				"fldDate", "fldDate", NativeDataTypes.SAS.DATE, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.TARGET, false), "DATE5.");
		fields.add(fldDate);
		Field fldDateTime = SASHelper.configureField(new Field("fldDateTime",
				"fldDateTime", "fldDateTime", NativeDataTypes.SAS.DATETIME,
				"12", "0", FieldKeyType.NOT_A_KEY, FieldType.TARGET, false),
				"DATETIME24.");
		fields.add(fldDateTime);
		Field fldTime = SASHelper.configureField(new Field("fldTime",
				"fldTime", "fldTime", NativeDataTypes.SAS.TIME, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.TARGET, false), "TIME.");
		fields.add(fldTime);
		Field fldYYYMMDD = SASHelper.configureField(new Field("fldYYYMMDD",
				"fldYYYMMDD", "fldYYYMMDD", NativeDataTypes.SAS.DATE, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.TARGET, false), "YYMMDD.");
		fields.add(fldYYYMMDD);

		sasTgt.setFields(fields);
		SASHelper
				.configureSASTarget(
						sasTgt,
						"TDATASET",
						"",
						"",
						"ShortTextIndex^2false^2^2fldShortText^3^1NumericIndex^2false^2^2fldNumeric15^3^1",
						"NumericIndex");

		return sasTgt;
	}

	/**
	 * Method to create ekko sap source
	 * 
	 * @param dbName
	 *            dbd name
	 */
	protected Source createSAPekkoSource(String dbName) {

		Source ekkoSource = null;

		List<Field> fields = new ArrayList<Field>();

		Field mandtField = new Field("MANDT", "Client", "Client",
				NativeDataTypes.SAP.CLNT, "3", "0", FieldKeyType.PRIMARY_KEY,
				FieldType.SOURCE, true);
		fields.add(mandtField);

		Field eblnField = new Field("EBELN", "Purchasing document number",
				"Purchasing document number", NativeDataTypes.SAP.CHAR, "10",
				"0", FieldKeyType.PRIMARY_FOREIGN_KEY, FieldType.SOURCE, true);
		fields.add(eblnField);

		Field bukrsField = new Field("BUKRS", "Company Code", "Company Code",
				NativeDataTypes.SAP.CHAR, "4", "0", FieldKeyType.NOT_A_KEY,
				FieldType.SOURCE, true);
		fields.add(bukrsField);

		Field pincrField = new Field("PINCR", "Item number interval",
				"Item number interval", NativeDataTypes.SAP.NUMC, "5", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(pincrField);

		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.SAP_R3,
				dbName);
		info.getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_TABLE_NAME, "sourcetable");
		info.getConnProps().setProperty(
				ConnectionPropsConstants.ORDER_BY_PORTS, "1");
		info.getConnProps().setProperty(ConnectionPropsConstants.SELECT_OPTION,
				"Select Single");
		info.getConnProps().setProperty(
				ConnectionPropsConstants.CONNECTIONNAME, "SAP_R3");
		ekkoSource = new Source("EKKO", "Purchasing Document Header11",
				"Purchasing Document Header1", "EKKO", info);
		ekkoSource.setDatabaseSubtype(StringConstants.SAP_TRANSPARENT_TABLE);
		ekkoSource.setFields(fields);
		return ekkoSource;
	}

	// DBD name as argument
	protected Source createSimpleSAPSource() {

		Source sapSrc = null;

		try {
			sapSrc = folder.fetchSourcesFromRepository(new INameFilter() {

				@Override
				public boolean accept(String name) {
					// TODO Auto-generated method stub
					return name.equals("A004");
				}
			}).get(0);
		} catch (RepoOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MapFwkReaderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sapSrc;
	}

	/**
	 * Method to create ekko sap source
	 * 
	 * @param dbName
	 *            dbd name
	 */
	protected Source createSAPekpoSource(String dbName) {
		Source ekkoSource = null;

		List<Field> fields = new ArrayList<Field>();
		Field mandtField = new Field("MANDT", "Client", "Client",
				NativeDataTypes.SAP.CLNT, "3", "0", FieldKeyType.NOT_A_KEY,
				FieldType.SOURCE, true);
		fields.add(mandtField);

		Field eblnField = new Field("EBELN", "Purchasing document number",
				"Purchasing document number", NativeDataTypes.SAP.CHAR, "10",
				"0", FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(eblnField);

		Field bukrsField = new Field("BUKRS", "Company Code", "Company Code",
				NativeDataTypes.SAP.CHAR, "4", "0", FieldKeyType.NOT_A_KEY,
				FieldType.SOURCE, true);
		fields.add(bukrsField);

		Field ktmngField = new Field("KTMNG", "Target quantity",
				"Target quantity", NativeDataTypes.SAP.QUAN, "13", "3",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(ktmngField);

		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.SAP_R3,
				dbName);
		info.getConnProps().setProperty(
				ConnectionPropsConstants.CONNECTIONNAME, "SAP_R3");
		ekkoSource = new Source("EKPO", "Purchasing Document Item1",
				"Purchasing Document Item1", "EKPO", info);
		ekkoSource.setDatabaseSubtype(StringConstants.SAP_TRANSPARENT_TABLE);
		ekkoSource.setFields(fields);
		return ekkoSource;
	}

	/**
	 * Method to create Job Info relational source for Teradata database
	 * 
	 * @param dbName
	 *            database name
	 */
	protected Source createTeradataJobSource(String dbName) {
		Source jobSource = null;

		List<Field> fields = new ArrayList<Field>();
		Field jobIDField = new Field("JOB_ID", "JOB_ID", "",
				NativeDataTypes.Teradata.VARCHAR, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(jobIDField);

		Field jobTitleField = new Field("JOB_TITLE", "JOB_TITLE", "",
				NativeDataTypes.Teradata.VARCHAR, "35", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(jobTitleField);

		Field minSalField = new Field("MIN_SALARY", "MIN_SALARY", "",
				NativeDataTypes.Teradata.DECIMAL, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(minSalField);

		Field maxSalField = new Field("MAX_SALARY", "MAX_SALARY", "",
				NativeDataTypes.Teradata.DECIMAL, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(maxSalField);

		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.Teradata,
				dbName);
		jobSource = new Source("JOBS", "JOBS", "This is JOBS table", "JOBS",
				info);
		jobSource.setFields(fields);
		return jobSource;
	}

	public Source createTabledataJobSource(String dbName) {
		Source jobSource = null;
		List<Field> fields = new ArrayList<Field>();
		Field inid = new Field("SCALAR_INPUT_Integration_Id", "JOB_ID", "",
				NativeDataTypes.Teradata.VARCHAR, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(inid);
		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.Teradata,
				dbName);
		jobSource = new Source("Table", "Table", "This is TableType table",
				"Table", info);
		jobSource.setFields(fields);
		return jobSource;
	}

	public Source createSAPCompanyCodeSource(String dbName) {
		Source jobSource = null;

		List<Field> fields = new ArrayList<Field>();
//		Field plvar = new Field("SCALAR_INPUT_PLVAR", "JOB_ID", "",
//				NativeDataTypes.Oracle.VARCHAR, "10", "0",
//				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
//		fields.add(plvar);

//		Field otype = new Field("SCALAR_INPUT_OTYPE", "JOB_TITLE", "",
//				NativeDataTypes.Oracle.LONG, "35", "0",
//				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
//		fields.add(otype);
//
//		Field objId = new Field("SCALAR_INPUT_OBJID", "MIN_SALARY", "",
//				NativeDataTypes.Oracle.CHAR, "6", "0", FieldKeyType.NOT_A_KEY,
//				FieldType.SOURCE, false);
//		fields.add(objId);
//
//		Field begDate = new Field("SCALAR_INPUT_BEGIN_DATE", "MAX_SALARY", "",
//				NativeDataTypes.Oracle.DATE, "6", "0", FieldKeyType.NOT_A_KEY,
//				FieldType.SOURCE, false);
//		fields.add(begDate);
//		Field endDate = new Field("SCALAR_INPUT_END_DATE", "JOB_ID", "",
//				NativeDataTypes.Oracle.DATE, "10", "0",
//				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
//		fields.add(endDate);
		Field inid = new Field("SCALAR_INPUT_Integration_Id", "JOB_ID", "",
				NativeDataTypes.Oracle.VARCHAR, "10", "0",
				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
		fields.add(inid);

		ConnectionInfo info = getRelationalConnInfo(SourceTargetType.Oracle,
				dbName);
		jobSource = new Source("COMPANYCODE", "COMPANYCODE",
				"This is COMPANYCODE table", "COMPANYCODE", info);
		jobSource.setFields(fields);
		return jobSource;
	}

	protected ConnectionInfo getRelationalConnInfo(SourceTargetType dbType,
			String dbName) {
		ConnectionInfo connInfo = null;
		connInfo = new ConnectionInfo(dbType);
		connInfo.getConnProps().setProperty(ConnectionPropsConstants.DBNAME,
				dbName);
		return connInfo;
	}

	/**
	 * Method to create relational target
	 */
	protected Target createRelationalTarget(SourceTargetType DBType, String name) {
		Target target = new Target(name, name, name, name, new ConnectionInfo(
				DBType));
		return target;
	}

	/**
	 * This method creates the target for the mapping
	 * 
	 * @return
	 */
	public Target createFlatFileTarget(String name) {
		Target tgt = new Target(name, name, "", name, new ConnectionInfo(
				SourceTargetType.Flat_File));
		tgt.getConnInfo()
				.getConnProps()
				.setProperty(ConnectionPropsConstants.FLATFILE_CODEPAGE,
						"MS1252");
		tgt.getConnInfo()
				.getConnProps()
				.setProperty(ConnectionPropsConstants.OUTPUT_FILENAME,
						name + ".out");
		return tgt;
	}

	/**
	 * This method creates the target for relational database
	 * 
	 * @param name
	 * @param relationalType
	 * @return
	 */
	public Target createRelationalTarget(String name,
			SourceTargetType relationalType) {
		Target tgt = new Target(name, name, "", name, new ConnectionInfo(
				relationalType));
		//tgt.setConflictResolution("RTR");
		List<Field> fields = new ArrayList<Field>();
		
			Field cust_keyField = new Field("cust_key", "cust_key", "",
					NativeDataTypes.SqlServer.INT, "10", "0",
					FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true);
			fields.add(cust_keyField); 
			
		
		Field Customer_idField = new Field("Customer_id", "Customer_id", "",
				NativeDataTypes.SqlServer.INT, "35", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(Customer_idField);

		Field LocationField = new Field("Location", "Location", "",
				NativeDataTypes.SqlServer.VARCHAR, "35", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(LocationField);

		Field flagField = new Field("flag", "flag", "",
				NativeDataTypes.SqlServer.INT, "20", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false);
		fields.add(flagField);
		
		//System.out.println("Here are the fields "+tgt.getConflictResolution() +""+tgt.getReferencedFields());
				tgt.setFields(fields);
				
		return tgt;
	}

	/**
	 * This method generates the output xml
	 * 
	 * @throws Exception
	 *             exception
	 */
	public void generateOutput() throws Exception {
		MapFwkOutputContext outputContext = new MapFwkOutputContext(
				MapFwkOutputContext.OUTPUT_FORMAT_XML,
				MapFwkOutputContext.OUTPUT_TARGET_FILE, mapFileName);

		try {
			intializeLocalProps();
		} catch (IOException ioExcp) {
			System.err.println(ioExcp.getMessage());
			System.err.println("Error reading pcconfig.properties file.");
			System.err
					.println("pcconfig.properties file is present in \\javamappingsdk\\samples directory");
			System.exit(0);
		}

		boolean doImport = false;
		if (runMode == 1)
			doImport = true;
		rep.save(outputContext, doImport);
		System.out.println("Mapping generated in " + mapFileName);
	}

	protected void setMapFileName(Mapping mapping) {
		StringBuffer buff = new StringBuffer();
		buff.append(System.getProperty("user.dir"));
		buff.append(java.io.File.separatorChar);
		buff.append(mapping.getName());
		buff.append(".xml");
		mapFileName = buff.toString();
	}

	protected void addFieldsToSource(Source src, List<Field> fields) {
		int size = fields.size();
		for (int i = 0; i < size; i++) {
			src.addField((Field) fields.get(i));
		}
	}

	protected ConnectionInfo getFlatFileConnectionInfo() {

		ConnectionInfo infoProps = new ConnectionInfo(
				SourceTargetType.Flat_File);
		infoProps.getConnProps().setProperty(
				ConnectionPropsConstants.FLATFILE_SKIPROWS, "1");
		infoProps.getConnProps().setProperty(
				ConnectionPropsConstants.FLATFILE_DELIMITERS, ";");
		infoProps.getConnProps().setProperty(
				ConnectionPropsConstants.DATETIME_FORMAT,
				"A  21 yyyy/mm/dd hh24:mi:ss");
		infoProps.getConnProps().setProperty(
				ConnectionPropsConstants.FLATFILE_QUOTE_CHARACTER, "DOUBLE");

		return infoProps;

	}

	protected ConnectionInfo getNetezzaConnectionInfo() {

		ConnectionInfo infoProps = new ConnectionInfo(SourceTargetType.Netezza);
		return infoProps;

	}

	protected ConnectionInfo getRelationalConnectionInfo(SourceTargetType dbType) {
		ConnectionInfo infoProps = new ConnectionInfo(dbType);
		infoProps.getConnProps().setProperty(
				ConnectionPropsConstants.CONNECTIONNAME, "PS4339");
		return infoProps;
	}

	// get an FTP connection
	protected ConnectionInfo getFTPConnectionInfo() {
		ConnectionInfo infoProps = new ConnectionInfo(SourceTargetType.FTP);
		infoProps.setConnectionName("ftp_con");
		return infoProps;
	}

	/**
	 * Method to get relational connection info object
	 */
	protected ConnectionInfo getRelationalConnectionInfo(
			SourceTargetType dbType, String dbName) {
		ConnectionInfo connInfo = new ConnectionInfo(dbType);
		connInfo.getConnProps().setProperty(ConnectionPropsConstants.DBNAME,
				dbName);
		return connInfo;
	}

	protected void intializeLocalProps() throws IOException {

		Properties properties = new Properties();
		String filename = "pcconfig.properties";
		InputStream propStream = getClass().getClassLoader()
				.getResourceAsStream(filename);

		if (propStream != null) {
			properties.load(propStream);
			rep.getRepoConnectionInfo()
					.setPcClientInstallPath(
							properties
									.getProperty(RepoPropsConstants.PC_CLIENT_INSTALL_PATH));
			rep.getRepoConnectionInfo()
					.setPcServerInstallPath(
							properties
									.getProperty(RepoPropsConstants.PC_SERVER_INSTALL_PATH));
			rep.getRepoConnectionInfo()
					.setTargetFolderName(
							properties
									.getProperty(RepoPropsConstants.TARGET_FOLDER_NAME));
			rep.getRepoConnectionInfo()
					.setTargetRepoName(
							properties
									.getProperty(RepoPropsConstants.TARGET_REPO_NAME));
			rep.getRepoConnectionInfo()
					.setRepoServerHost(
							properties
									.getProperty(RepoPropsConstants.REPO_SERVER_HOST));
			rep.getRepoConnectionInfo().setAdminPassword(
					properties.getProperty(RepoPropsConstants.ADMIN_PASSWORD));
			rep.getRepoConnectionInfo().setAdminUsername(
					properties.getProperty(RepoPropsConstants.ADMIN_USERNAME));
			rep.getRepoConnectionInfo()
					.setRepoServerPort(
							properties
									.getProperty(RepoPropsConstants.REPO_SERVER_PORT));
			rep.getRepoConnectionInfo().setServerPort(
					properties.getProperty(RepoPropsConstants.SERVER_PORT));
			rep.getRepoConnectionInfo().setDatabaseType(
					properties.getProperty(RepoPropsConstants.DATABASETYPE));
			rep.getRepoConnectionInfo().setSecurityDomain(
					properties.getProperty(RepoPropsConstants.SECURITY_DOMAIN));
			rep.getRepoConnectionInfo().setKerberosMode(
					Boolean.parseBoolean(properties.getProperty(RepoPropsConstants.KERBEROS_ENABLED)));

			if (properties.getProperty(RepoPropsConstants.PMREP_CACHE_FOLDER) != null)
				rep.getRepoConnectionInfo()
						.setPmrepCacheFolder(
								properties
										.getProperty(RepoPropsConstants.PMREP_CACHE_FOLDER));
		} else {
			throw new IOException(
					"pcconfig.properties file not found.Add Directory containing pcconfig.properties to ClassPath");
		}
	}

	public boolean validateRunMode(String value) {
		int val = Integer.parseInt(value);
		if (val > 1 || val < 0) {
			printUsage();
			return false;
		} else {
			runMode = val;
			return true;
		}
	}

	public void printUsage() {
		String errorMsg = "***************** USAGE *************************\n";
		errorMsg += "\n";
		errorMsg += "Valid arguments are:\n";
		errorMsg += "0       => Generate powermart xml file\n";
		errorMsg += "1       => Import powermart xml file into PowerCenter Repository\n";
		errorMsg += "\n";
		errorMsg += "********************************************************\n";
		System.out.println(errorMsg);
	}

}

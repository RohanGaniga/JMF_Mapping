/*
 * Expression.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformGroup;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContextFactory;

/**
 * This example applies a simple expression transformation on the Employee table
 * and writes to a target
 * 
 */
public class Type2SCDmapping extends Type2SCDBase {
	// /////////////////////////////////////////////////////////////////////////////////////
	// Instance variables
	// /////////////////////////////////////////////////////////////////////////////////////
	protected Source employeeSrc;
	protected Target outputTarget;
	protected Target outputTarget2;
	protected Source LkpEmployeeId;
	protected Target outputTarget3;

	/**
	 * Create sources
	 */
	protected void createSources() {
		LkpEmployeeId = this.createLkpEmployeeIdSource("Lkp");
		folder.addSource(LkpEmployeeId);
		employeeSrc = this.createOracleJobSource("Cust");
		folder.addSource( employeeSrc );
	}

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createRelationalTarget("Customer_Dim3", SourceTargetType.Microsoft_SQL_Server);
		outputTarget2 = this.createRelationalTarget( "Customer_Dim31",SourceTargetType.Microsoft_SQL_Server );
		outputTarget3 = this.createRelationalTarget( "Customer_Dim32",SourceTargetType.Microsoft_SQL_Server );
	}
	public void createMappings() throws Exception {
		// create a mapping
		mapping = new Mapping( "Type2SCD_MAPPING", "mapping", "Testing Expression sample" );
		setMapFileName( mapping );
		TransformHelper helper = new TransformHelper( mapping );
		// creating DSQ Transformation
		OutputSet outSet = helper.sourceQualifier( employeeSrc );
		RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );


		// create an expression Transformation
		// the fields LastName and FirstName are concataneted to produce a new
		// field fullName
		String New_flag = "integer(10,0) New_flag=IIF(ISNULL(Cust_Key), 1,0)";
		TransformField outField = new TransformField(New_flag);
		String Changed_flag = "integer(10,0) Changed_flag = IIF( NOT ISNULL(Cust_Key) AND Location1 != IN_Location, 1, 0)";
		TransformField outField2 = new TransformField(Changed_flag);
		List<TransformField> transFields = new ArrayList<TransformField>();
		transFields.add( outField );
		transFields.add( outField2 );
		//transFields.add( outField2 );









		/*
		TransformField cost = new TransformField(
				"decimal(15,0) total_salary = SUM(MIN_SALARY)");

		RowSet aggRS = (RowSet) helper.aggregate(expRS, cost,
				new String[] { "JOB_ID" }, "agg_transform").getRowSets()
				.get(0);

		PortPropagationContext dsqRSContext = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "JOB_ID" });*/

		PortPropagationContext exprstofilteronecontext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[] { "New_flag"});
		PortPropagationContext exprstofiltertwocontext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[] { "Changed_flag"});

		PortPropagationContext dsqRSContext = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "" });
		PortPropagationContext toExprContext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[] { "Name","Location"});
		outSet = helper.lookup(dsqRS, LkpEmployeeId,
				"Customer_id = In_customer_id",
				"Lkp");
		RowSet lookupRS = (RowSet) outSet.getRowSets().get(0);
		PortPropagationContext lkpRSContext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[] {"Cust_key"});

		List<InputSet> inputSsetsforlookuptoex = new ArrayList<InputSet>();
		inputSsetsforlookuptoex.add(new InputSet(dsqRS,toExprContext));
		inputSsetsforlookuptoex.add(new InputSet(lookupRS));
		RowSet expRS = (RowSet) helper.expression( inputSsetsforlookuptoex, transFields, "exp_transform" ).getRowSets()
				.get( 0 );

		/*List<InputSet> inputSsetsforlookuptoex1 = new ArrayList<InputSet>();
		inputSsetsforlookuptoex1.add(new InputSet(dsqRS));;*/


		/*RowSet lookupRS1 = (RowSet) outSet.getRowSets().get(0);
		PortPropagationContext lkpRSContext1 = PortPropagationContextFactory
				.getContextForIncludeCols(new String[] { "Cust_key","Name", "Location"});*/


		List<InputSet> inputSets = new ArrayList<InputSet>();


		// remove
		inputSets.add(new InputSet(dsqRS,dsqRSContext));	
		// Manufacturer_id
		inputSets.add(new InputSet(lookupRS, lkpRSContext));
		//inputSets.add(new InputSet(lookupRS1,lkpRSContext1));
		inputSets.add(new InputSet(expRS, exprstofilteronecontext));

		//inputSets.add(new InputSet(expRS, exprstofiltertwocontext));


		List<InputSet> inputSets2 = new ArrayList<InputSet>();
		// remove
		inputSets2.add(new InputSet(dsqRS,dsqRSContext));											// Manufacturer_id
		inputSets2.add(new InputSet(lookupRS, lkpRSContext));
		inputSets2.add(new InputSet(expRS, exprstofiltertwocontext));

		//RowSet expRS = (RowSet) helper.expression( inputSets, transFields, "exp_transform" ).getRowSets()
		//		.get( 0 );
		RowSet filterRS = (RowSet) helper.filter( inputSets, "New_flag=1", "FIL_INS" )
				.getRowSets().get( 0 );
		RowSet filterRS1 = (RowSet) helper.filter( inputSets2, "Changed_flag=1", "FIL_changed" )
				.getRowSets().get( 0 );
		/*// write to target
		//mapping.writeTarget( inputSets, outputTarget );

		// Create a TransformGroup
		List<TransformGroup> transformGrps = new ArrayList<TransformGroup>();
		TransformGroup transGrp = new TransformGroup( "insert", "flag = 'ins'" );
		transformGrps.add( transGrp );
		transGrp = new TransformGroup( "update", "flag = 'upd'" );
		transformGrps.add( transGrp );

		// create a Router Transformation
		OutputSet routerOutputSet = helper.router( expRS, transformGrps,
				"Router_transform" );
		System.out.println(helper.router(expRS, transformGrps,
				"Router_transform").getRowSets().get(0).getBusinessName());

		PortPropagationContext insertTargetContext = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] {"S_key1"});

		PortPropagationContext updateTargetContext = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] {"flag2"});

		PortPropagationContext insertTargetContext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[]{""});

		PortPropagationContext updateTargetContext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[]{""});

		RowSet outRS = routerOutputSet.getRowSet( "insert" );
		if(outRS != null ){
			RowSet filterRS2 = (RowSet) helper.updateStrategy( outRS,
					"DD_INSERT", "upd_ins_transform" )
					.getRowSets().get( 0 );
			//System.out.println();outputTarget.getBusinessName();
			InputSet finalSetInsert = new InputSet(filterRS,insertTargetContext);*/
		//mapping.getTargets().add(outputTarget);
		/*mapping.writeTarget(finalSetInsert, outputTarget);

		RowSet out2RS = routerOutputSet.getRowSet( "update" );
		if(out2RS != null ){
			RowSet filterRS3 = (RowSet) helper.updateStrategy( out2RS,
					"DD_UPDATE", "upd_updateStrategy_transform" )
					.getRowSets().get( 0 );
			//InputSet finalSetUpdate = new InputSet(filterRS,updateTargetContext);
			mapping.writeTarget(filterRS, outputTarget2);
	//	}
		 */		
		PortPropagationContext excludingfieldfromfiltercontext = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "New_flag","Changed_flag","cust_key"});
		PortPropagationContext excludingfieldfromsecondfiltercontext = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "New_flag","Changed_flag"});


		List<InputSet> inputsetforfirstfilter = new ArrayList<InputSet>();
		// remove
		inputsetforfirstfilter.add(new InputSet(filterRS,excludingfieldfromfiltercontext));



		List<InputSet> inputsetforsecondfilter = new ArrayList<InputSet>();
		// remove
		inputsetforsecondfilter.add(new InputSet(filterRS1,excludingfieldfromsecondfiltercontext));
		
		String flag1 = "integer(10,0) flag = 0";
		TransformField outField4 = new TransformField(flag1);
		List<TransformField> filtertoup = new ArrayList<TransformField>();
		filtertoup.add( outField4 );
		RowSet expRS2 = (RowSet) helper.expression( filterRS1, filtertoup, "exp_chang_flag" ).getRowSets()
				.get( 0 );
		

		RowSet firstUpdateStrRS = (RowSet) helper.updateStrategy( inputsetforfirstfilter,
				" DD_INSERT", "upd_ins_Strategy_transform" )
				.getRowSets().get( 0 );
		RowSet SecondUpdateStrRS = (RowSet) helper.updateStrategy( inputsetforsecondfilter,
				" DD_UPDATE", "upd_upd_Strategy_transform" )
				.getRowSets().get( 0 );
		RowSet exptoupdatestr =(RowSet) helper.updateStrategy(expRS2, 
				 "DD_UPDATE", "UPD_CH_UPD")
				.getRowSets().get(0);
		
		// create a Sequence Generator Transformation
		RowSet seqGenRS = (RowSet) helper.sequenceGenerator( "sequencegenerator_transform" )
				.getRowSets().get( 0 );
		
		
		
		String flag = "integer(10,0) flag=1";
		TransformField outField3 = new TransformField(flag);
		List<TransformField> sequenceexp = new ArrayList<TransformField>();
				sequenceexp.add( outField3);
		RowSet expRS1 = (RowSet) helper.expression( seqGenRS, sequenceexp, "exp_flag" ).getRowSets()
				.get( 0 );
		
		PortPropagationContext exptotgt = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "currval"});
		
		List<InputSet> inputSsetsforexptotarget = new ArrayList<InputSet>();
		inputSsetsforexptotarget.add(new InputSet(expRS1,exptotgt));
		inputSsetsforexptotarget.add(new InputSet(firstUpdateStrRS));
		
	
		
		
		

		PortPropagationContext sequencegenerator = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "currval"});
		
		PortPropagationContext exptoupdatestr1 = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "Changed_flag", "customer_id", "Location"});
		
		PortPropagationContext filtertoupdatestr1 = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "cust_key"});
		
		List<InputSet> inputsetsforupdatetotarget3 = new ArrayList<InputSet>();
		inputsetsforupdatetotarget3.add(new InputSet(expRS1,exptotgt));
		inputsetsforupdatetotarget3.add(new InputSet(SecondUpdateStrRS,filtertoupdatestr1));
		
		List<InputSet> inputsetsforupdatetotarget2 = new ArrayList<InputSet>();
		inputsetsforupdatetotarget2.add(new InputSet(exptoupdatestr,exptoupdatestr1));
		
		List<InputSet> vinSets = new ArrayList<InputSet>();
		vinSets.add( new InputSet( firstUpdateStrRS ) );
		vinSets.add( new InputSet( seqGenRS,sequencegenerator ) );


		/* List<InputSet> sequencegenerator_transform = new ArrayList<InputSet>();
			// remove
	        sequencegenerator_transform.add(new InputSet(seqGenRS,sequencegenerator));*/

		// write to target
		// mapping.writeTarget( seqGenRS, outputTarget );
		mapping.writeTarget(inputSsetsforexptotarget,outputTarget);
		mapping.writeTarget(inputsetsforupdatetotarget3, outputTarget2);
		mapping.writeTarget(inputsetsforupdatetotarget2, outputTarget3);
		folder.addMapping( mapping );
	}



	public static void main( String args[] ) {
		try {
			Type2SCDmapping expressionTrans = new Type2SCDmapping();
			if (args.length > 0) {
				if (expressionTrans.validateRunMode( args[0] )) {
					expressionTrans.execute();

				}
			} else {
				expressionTrans.printUsage();
			}



			BufferedReader br = new BufferedReader(new FileReader(new File("C:\\Users\\Admin\\workspace1\\Sample\\Type2SCD_MAPPING.xml")));
			String line = "";
			List<String> output = new ArrayList<>();
			String[] strArr = null;
			int i = 0;
			int startPoint = 0;
			String spaces = "";
			int flag = 0;
			yo:while((line = br.readLine()) != null) {
				i = 0;
				/*if(line.contains("TARGETFIELD")) {
					startPoint = line.indexOf("<");
					spaces = line.substring(1, startPoint-1);
					strArr = null;
					strArr = line.split("\\s+");
					line = "";
					while(i<strArr.length){
						if(strArr[i].contains("NAME")) {
							if(strArr[i].matches(".*\\d.*")){
								continue yo;
							}
						}
						if(i == 0){
							line = spaces+strArr[i] + " ";
						}
						else {
							line = line + " "+strArr[i];
						}
						i++;
					}
				}*/
				if(line.contains("CONNECTOR") && line.contains("FROMINSTANCE=\"exp_flag\"")){
					startPoint = line.indexOf("<");
					spaces = line.substring(1, startPoint-1);
					System.out.println(startPoint);
					strArr = null;
					strArr = line.split("\\s+");
					line = "";
					while(i<strArr.length){
						if(strArr[i].contains("TOFIELD")) {

							strArr[i] = strArr[i].replaceAll("NEXTVAL","cust_key");
						}
						if(i == 0){
							line = spaces+strArr[i] + " ";
						}
						else {
							line = line + " "+strArr[i];
						}
						i++;
					}
				}

				/*if(line.contains("TRANSFORMATION NAME=\"exp_transform\"")) {
					flag = 1;
					output.add(line);
					continue;
				}
				if(flag == 1) {
					startPoint = line.indexOf("<");
					System.out.println("hgjhghj"+startPoint);
					spaces = line.substring(1, startPoint-1);
					
					//System.out.println("line"+line);
					strArr = null;
					strArr = line.split("\\s+");
					line = "";
					while(i<strArr.length){
						if(strArr[i].contains("NAME=\"Location\"")) {
							flag = 0;
						}
						if( strArr[i].contains("NAME=\"Location\"")) {
							strArr[i] = strArr[i].replace("=\"", "=\"SRC_");
						}
						if(i == 0){
							line = spaces+strArr[i] + " ";
						}
						else {
							line = line + " "+strArr[i];
						}
						i++;
						
					}
				}
				if(line.contains("CONNECTOR") && line.contains("FROMINSTANCE=\"SQ_customers\"") && line.contains("TOINSTANCE=\"exp_transform\"")){
					startPoint = line.indexOf("<");
					spaces = line.substring(1, startPoint-1);
					//	System.out.println(startPoint);
					strArr = null;
					strArr = line.split("\\s+");
					line = "";
					while(i<strArr.length){
						if( strArr[i].contains("TOFIELD=\"Location\"")) {
							strArr[i] = strArr[i].replace("=\"", "=\"SRC_");
						}
						if(i == 0){
							line = spaces+strArr[i] + " ";
						}
						else {
							line = line + " "+strArr[i];
						}
						i++;
					}
				}*/
				output.add(line);
			}
			br.close();
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File("C:\\Users\\Admin\\workspace1\\Sample\\Type2SCD_MAPPING.xml")));
			for(String s : output){
				writer.write(s);
				writer.newLine();
			}
			writer.flush();
			writer.close();
			//List<String> output = new ArrayList<String>();


		} 
		catch (Exception e) {
			e.printStackTrace();
			System.err.println( "Exception is: " + e.getMessage() );
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
	 */
	protected void createSession() throws Exception {
		session = new Session( "Session_For_Expression", "Session_For_Expression",
				"This is session for expression" );
		session.setMapping( this.mapping );
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow( "Workflow_for_Expression", "Workflow_for_Expression",
				"This workflow for expression" );
		workflow.addSession( session );
		folder.addWorkFlow( workflow );
	}
}

/*
 * Expression.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.ArrayList;
import java.util.List;

import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContextFactory;

/**
 * This example applies a simple expression transformation on the Employee table
 * and writes to a target
 * 
 */
public class Expression2 extends Base {
	// /////////////////////////////////////////////////////////////////////////////////////
	// Instance variables
	// /////////////////////////////////////////////////////////////////////////////////////
	protected Source employeeSrc;
	protected Target outputTarget;
	protected Source LkpEmployeeId;

	/**
	 * Create sources
	 */
	protected void createSources() {
		LkpEmployeeId = this.createLkpEmployeeIdSource();
		folder.addSource(LkpEmployeeId);
		employeeSrc = this.createOracleJobSource("Manideep_Test");
		folder.addSource( employeeSrc );
	}

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createFlatFileTarget( "Expression_Output" );
	}

	public void createMappings() throws Exception {
		// create a mapping
		mapping = new Mapping( "ExpressionMapping", "mapping", "Testing Expression sample" );
		setMapFileName( mapping );
		TransformHelper helper = new TransformHelper( mapping );
		// creating DSQ Transformation
		OutputSet outSet = helper.sourceQualifier( employeeSrc );
		RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
		// create an expression Transformation
		// the fields LastName and FirstName are concataneted to produce a new
		// field fullName
		String expr = "string(80, 0) fullName= firstName || lastName";
		TransformField outField = new TransformField( expr );
		/*String expr2 = "integer(10,0) YEAR_out=IIF(EmployeeID=0,2000,2001)";
		TransformField outField2 = new TransformField( expr2 );*/
		List<TransformField> transFields = new ArrayList<TransformField>();
		transFields.add( outField );
		//transFields.add( outField2 );
		RowSet expRS = (RowSet) helper.expression( dsqRS, transFields, "exp_transform" ).getRowSets()
				.get( 0 );
		
		TransformField cost = new TransformField(
				"decimal(15,0) total_salary = SUM(MIN_SALARY)");
		
		RowSet aggRS = (RowSet) helper.aggregate(expRS, cost,
				new String[] { "JOB_ID" }, "agg_transform").getRowSets()
				.get(0);
		
		PortPropagationContext dsqRSContext = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "JOB_ID" });
		outSet = helper.lookup(aggRS, LkpEmployeeId,
				"JOB_ID = in_JOB_ID",
				"Lookup_LkpEmployeeId_Table");
		RowSet lookupRS = (RowSet) outSet.getRowSets().get(0);
		PortPropagationContext lkpRSContext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[] { "JOB_Name" });
		List<InputSet> inputSets = new ArrayList<InputSet>();
		inputSets.add(new InputSet(aggRS, dsqRSContext)); // remove
															// Manufacturer_id
		inputSets.add(new InputSet(lookupRS, lkpRSContext));
		
		// write to target
		mapping.writeTarget( inputSets, outputTarget );
		folder.addMapping( mapping );
	}

	public static void main( String args[] ) {
		try {
			Expression2 expressionTrans = new Expression2();
			if (args.length > 0) {
				if (expressionTrans.validateRunMode( args[0] )) {
					expressionTrans.execute();
				}
			} else {
				expressionTrans.printUsage();
			}
		} catch (Exception e) {
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

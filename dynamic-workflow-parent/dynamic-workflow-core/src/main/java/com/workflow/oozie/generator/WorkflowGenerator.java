package com.workflow.oozie.generator;

/*
* 
* Interface for webservice interaction, request parsing, create workflows for oozie orchestration.
* 
*/

import java.io.IOException;

import com.workflow.oozie.model.DynamicWorkflowConfig;

public interface WorkflowGenerator {

	/*
	 * This method will generate & return the work flow XML file.
	 */

	public String generateWorkFlow(DynamicWorkflowConfig dynamicWorkflowConfigObj);

}

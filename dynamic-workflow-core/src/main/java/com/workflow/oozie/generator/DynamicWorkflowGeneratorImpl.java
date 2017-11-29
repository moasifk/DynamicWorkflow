package com.workflow.oozie.generator;

import java.io.IOException;
import java.util.Arrays;

import com.workflow.oozie.model.DynamicWorkflowConfig;
import com.workflow.oozie.nodes.OozieNodeFactory;
import com.workflow.oozie.nodes.WorkFlowApp;

public class DynamicWorkflowGeneratorImpl implements WorkflowGenerator{
	
	private static final String WORKFLOW_NAME = "workflow-name";
	
	OozieNodeFactory oozieNodeFactory;
	WorkFlowApp workFlowApp;
	OozieNodeCreator nodeCreator;
	
	/*public static void main(String[] args) {
		DynamicWorkflowGeneratorImpl obj = new DynamicWorkflowGeneratorImpl();
		try {
			obj.generateWorkFlow(args);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/

	@Override
	public void generateWorkFlow(DynamicWorkflowConfig dynamicWorkflowConfigObj) throws IOException {
		oozieNodeFactory = new OozieNodeFactory();
		workFlowApp = oozieNodeFactory.createWorkFlowApp();
		
		workFlowApp.setName(WORKFLOW_NAME);
		workFlowApp.setGlobal(nodeCreator.createGlobalNode());
		/*workFlowApp.getDecisionOrForkOrJoin().add(
				nodeCreator.createSSHActionNode("SSH-action-node-name", "ssh-host",
						"ssh-command", Arrays.asList(args), "ssh-ok-name", "ssh-error-name"));*/
//		workFlowApp.getDecisionOrForkOrJoin().add(nodeCreator.createSparkActionNode(actionNodeName, jobTracker, nameNode, master, mode, applicationName, mainClass, inputPathList, nextActionName, jarName, args, okayNodeName, errorNodeName))
		
	}

}

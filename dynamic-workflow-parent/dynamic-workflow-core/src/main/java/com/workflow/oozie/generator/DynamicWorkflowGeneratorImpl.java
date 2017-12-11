package com.workflow.oozie.generator;

import java.io.IOException;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import com.workflow.oozie.model.DynamicWorkflowConfig;
import com.workflow.oozie.model.SSHNodeDetails;
import com.workflow.oozie.model.SparkNodeDetails;
import com.workflow.oozie.nodes.ActionNode;
import com.workflow.oozie.nodes.OozieNodeFactory;
import com.workflow.oozie.nodes.WorkFlowApp;

public class DynamicWorkflowGeneratorImpl implements WorkflowGenerator {

	private static final String WORKFLOW_NAME = "workflow-name";

	OozieNodeFactory oozieNodeFactory;
	WorkFlowApp workFlowApp;
	OozieNodeCreator nodeCreator;
	
	public String generateWorkFlow(DynamicWorkflowConfig dynamicWorkflowConfigObj) throws IOException {
		StringWriter writer = new StringWriter();
		if (dynamicWorkflowConfigObj != null) {
			oozieNodeFactory = new OozieNodeFactory();
			workFlowApp = oozieNodeFactory.createWorkFlowApp();
			workFlowApp.setName(WORKFLOW_NAME);
			nodeCreator = new OozieNodeCreator();
			
			// Adding global node details 
			if (dynamicWorkflowConfigObj.getGlobalNodeDetails() != null) {
				workFlowApp.setGlobal(nodeCreator.createGlobalNode(dynamicWorkflowConfigObj.getGlobalNodeDetails()));
			}
			
			if (dynamicWorkflowConfigObj.getSSHNodeDetails() != null) {
				SSHNodeDetails sshNodeDetails = dynamicWorkflowConfigObj.getSSHNodeDetails();
				workFlowApp.getDecisionOrForkOrJoin().add(nodeCreator.createSSHActionNode(sshNodeDetails.getNodeName(),
						sshNodeDetails.getHost(), sshNodeDetails.getCommand(), sshNodeDetails.getArgs(), sshNodeDetails.getOkToName(), sshNodeDetails.getErrorToName()));
			}
			
			if (dynamicWorkflowConfigObj.getSparkNodeDetails() != null) {
				SparkNodeDetails sparkNodeDetails = dynamicWorkflowConfigObj.getSparkNodeDetails();
				workFlowApp.getDecisionOrForkOrJoin().add(nodeCreator.createSparkActionNode(sparkNodeDetails.getNodeName(),
						sparkNodeDetails.getJobTracker(), sparkNodeDetails.getNameNode(), sparkNodeDetails.getMaster(), sparkNodeDetails.getMode(),
						sparkNodeDetails.getApplicationName(), sparkNodeDetails.getClassName(), sparkNodeDetails.getJar(), sparkNodeDetails.getArgs(), sparkNodeDetails.getOkayToName(),
						sparkNodeDetails.getErrorToName()));
			}
			
			// Adding End node details to workflow.xml
			workFlowApp.setEnd(nodeCreator.createEndNode(dynamicWorkflowConfigObj.getEndNodeName()));
						
			try {
				JAXBContext jc = JAXBContext.newInstance(WorkFlowApp.class, ActionNode.class);

				Marshaller marshaller;

				marshaller = jc.createMarshaller();

				marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

				marshaller.setProperty("jaxb.encoding", "UTF-8");
				// This is to handle the unicode and special characters.
				// marshaller.setProperty(CharacterEscapeHandler.class.getName(),new
				// XmlCharacterHandler());
				JAXBElement<WorkFlowApp> jaxbElem = new JAXBElement<WorkFlowApp>(
						new QName("uri:oozie:workflow:0.4", "workflow-app"), WorkFlowApp.class, workFlowApp);
				marshaller.marshal(jaxbElem, writer);
			} catch (JAXBException e) {
				e.printStackTrace();
			}
		}
		return writer.toString();
	}


}

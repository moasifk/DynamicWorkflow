package com.workflow.oozie.generator;

import java.io.IOException;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import com.workflow.oozie.model.DynamicWorkflowConfig;
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
			if (dynamicWorkflowConfigObj.getGlobalNodeDetails() != null) {
				workFlowApp.getDecisionOrForkOrJoin()
						.add(nodeCreator.createGlobalNode(dynamicWorkflowConfigObj.getGlobalNodeDetails()));
			}

			/*
			 * workFlowApp.getDecisionOrForkOrJoin().add(
			 * nodeCreator.createSSHActionNode("SSH-action-node-name", "ssh-host",
			 * "ssh-command", Arrays.asList(args), "ssh-ok-name", "ssh-error-name"));
			 */
			// workFlowApp.getDecisionOrForkOrJoin().add(nodeCreator.createSparkActionNode(actionNodeName,
			// jobTracker, nameNode, master, mode, applicationName, mainClass,
			// inputPathList, nextActionName, jarName, args, okayNodeName,
			// errorNodeName))

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

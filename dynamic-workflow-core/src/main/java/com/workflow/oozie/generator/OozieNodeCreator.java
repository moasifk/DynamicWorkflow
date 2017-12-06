package com.workflow.oozie.generator;

import java.util.Iterator;
import java.util.List;

import com.workflow.oozie.model.GlobalNodeDetails;
import com.workflow.oozie.model.Property;
import com.workflow.oozie.nodes.ActionNode;
import com.workflow.oozie.nodes.ActionTransition;
import com.workflow.oozie.nodes.Configuration;
import com.workflow.oozie.nodes.Global;
import com.workflow.oozie.nodes.OozieNodeFactory;
import com.workflow.oozie.nodes.SSH;
import com.workflow.oozie.nodes.Spark;

public class OozieNodeCreator {

	private static final String EMPTY_STRING = "";
	private static final String GLOBAL_NODE_PROPERTY_VALUE = "global-node-property-value";
	private static final String GLOBAL_NODE_PROPERTY_NAME = "global-node-property-name";
	OozieNodeFactory oozieNodeFactory;
	String jobTracker;
	String nameNode;

	public OozieNodeCreator() {
		oozieNodeFactory = new OozieNodeFactory();
	}

	/*
	 * Adding <global> node to workflow Here we define the configuration details
	 */
	public Global createGlobalNode(GlobalNodeDetails globalNodeDetails) {
		Global globalNode = oozieNodeFactory.createGlobal();
		Configuration config = oozieNodeFactory.createConfiguration();
		globalNode.setJobTracker(globalNodeDetails.getJobTracker());
		globalNode.setNameNode(globalNodeDetails.getNameNode());
		List<Property> globalNodeProperties = globalNodeDetails.getProperties();
		Iterator<Property> propertyIterator = globalNodeProperties.iterator();
		while (propertyIterator.hasNext()) {
			Property property = propertyIterator.next();
			Configuration.Property prop = new Configuration.Property();
			prop.setName(property.getPropertyName());
			prop.setValue(property.getPropertyValue());
			config.getProperty().add(prop);
		}
		globalNode.setConfiguration(config);
		return globalNode;
	}

	public ActionNode createSSHActionNode(String actionName, String host, String command, List<String> args,
			String okayNodeName, String errorNodeName) {
		ActionNode action = oozieNodeFactory.createActionNode();
		action.setName(actionName);
		SSH sshAction = oozieNodeFactory.createSSH();
		sshAction.setXmlns("uri:oozie:ssh-action:0.2");
		sshAction.setHost(host);
		sshAction.setCommand(command);
		Iterator<String> argsItr = args.iterator();
		while (argsItr.hasNext()) {
			sshAction.getArg().add(argsItr.next());
		}
		action.setSsh(sshAction);
		setOkTransition(action, okayNodeName);
		setErrorTransition(action, errorNodeName);
		return action;
	}

	private void setOkTransition(ActionNode act, String nodeName) {

		if (act != null) {
			ActionTransition okCallWfTrans = oozieNodeFactory.createActionTransition();
			okCallWfTrans.setTo(nodeName);
			act.setOk(okCallWfTrans);
		}
	}

	private void setErrorTransition(ActionNode act, String nodeName) {

		if (act != null) {
			ActionTransition errorCallWfTrans = oozieNodeFactory.createActionTransition();
			errorCallWfTrans.setTo(nodeName);
			act.setError(errorCallWfTrans);
		}
	}

	public ActionNode createSparkActionNode(String actionNodeName, String jobTracker, String nameNode, String master,
			String mode, String applicationName, String mainClass, List<String> inputPathList, String nextActionName,
			String jarName, List<String> args, String okayNodeName, String errorNodeName) {

		ActionNode action = oozieNodeFactory.createActionNode();
		action.setName(actionNodeName);
		Spark sparkAction = oozieNodeFactory.createSpark();
		sparkAction.setXmlns("uri:oozie:spark-action:0.1");
		sparkAction.setJobTracker(jobTracker);
		sparkAction.setNameNode(nameNode);
		sparkAction.setMaster(master);
		sparkAction.setMode(mode);
		sparkAction.setName(applicationName);
		sparkAction.setClazz(mainClass);
		sparkAction.setJar(jarName);
		sparkAction.setSparkOpts(setSparkOptions());
		Iterator<String> argsItr = args.iterator();
		while (argsItr.hasNext()) {
			sparkAction.getArg().add(argsItr.next());
		}

		Iterator<String> listItr = inputPathList.iterator();
		while (listItr.hasNext()) {
			sparkAction.getArg().add(listItr.next());
		}

		setOkTransition(action, okayNodeName);
		setErrorTransition(action, errorNodeName);
		action.setSpark(sparkAction);
		return action;
	}

	private String setSparkOptions() {
		String sparkOpts = EMPTY_STRING;
		sparkOpts = "--queue " + "queue-name" + " --files " + "file-path" + " --driver-memory " + "driver-memory"
				+ " --executor-memory " + "executor-memory" + " --executor-cores " + "executor-cores";
		return sparkOpts;
	}

}

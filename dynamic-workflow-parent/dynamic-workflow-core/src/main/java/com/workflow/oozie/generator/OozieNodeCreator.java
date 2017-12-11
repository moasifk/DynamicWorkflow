package com.workflow.oozie.generator;

import java.util.Iterator;
import java.util.List;

import com.workflow.oozie.model.Arg;
import com.workflow.oozie.model.ConfigurationProperties;
import com.workflow.oozie.model.DeleteArg;
import com.workflow.oozie.model.GlobalNodeDetails;
import com.workflow.oozie.model.MkdirArg;
import com.workflow.oozie.model.PrepareNode;
import com.workflow.oozie.nodes.ActionNode;
import com.workflow.oozie.nodes.ActionTransition;
import com.workflow.oozie.nodes.Configuration;
import com.workflow.oozie.nodes.Delete;
import com.workflow.oozie.nodes.End;
import com.workflow.oozie.nodes.Global;
import com.workflow.oozie.nodes.JavaAction;
import com.workflow.oozie.nodes.Kill;
import com.workflow.oozie.nodes.Mkdir;
import com.workflow.oozie.nodes.OozieNodeFactory;
import com.workflow.oozie.nodes.Prepare;
import com.workflow.oozie.nodes.SSH;
import com.workflow.oozie.nodes.Spark;

public class OozieNodeCreator {

	private static final String EMPTY_STRING = "";
	OozieNodeFactory oozieNodeFactory;

	public OozieNodeCreator() {
		oozieNodeFactory = new OozieNodeFactory();
	}

	/**
	 * Adding <global> node to workflow Here we define the configuration details
	 * @param globalNodeDetails
	 * @return Global
	 */
	public Global createGlobalNode(GlobalNodeDetails globalNodeDetails) {
		Global globalNode = oozieNodeFactory.createGlobal();
		Configuration config = oozieNodeFactory.createConfiguration();
		globalNode.setJobTracker(globalNodeDetails.getJobTracker());
		globalNode.setNameNode(globalNodeDetails.getNameNode());
		List<ConfigurationProperties> globalNodeProperties = globalNodeDetails.getConfigProperties();
		Iterator<ConfigurationProperties> propertyIterator = globalNodeProperties.iterator();
		while (propertyIterator.hasNext()) {
			ConfigurationProperties property = propertyIterator.next();
			Configuration.Property prop = new Configuration.Property();
			prop.setName(property.getPropertyName());
			prop.setValue(property.getPropertyValue());
			config.getProperty().add(prop);
		}
		globalNode.setConfiguration(config);
		return globalNode;
	}
	
	/**
	 * This is to create a END action node
	 * @param endNodeName
	 * @return End
	 */
	public End createEndNode(String endNodeName) {
		End endNode = oozieNodeFactory.createEnd();
		endNode.setName(endNodeName);
		return endNode;
	}
	
	/**
	 * This creates a KILL action node
	 * @param killNodeName
	 * @param killMessage
	 * @return Kill
	 */
	public Kill createKillNode(String killNodeName, String killMessage) {
		Kill killNode = oozieNodeFactory.createKill();
		killNode.setName(killNodeName);
		killNode.setMessage(killMessage);
		return killNode;
	}

	/**
	 * This method creates a SSH action node in oozie workflow
	 * @param actionName
	 * @param host
	 * @param command
	 * @param args
	 * @param okayNodeName
	 * @param errorNodeName
	 * @return ActionNode
	 */
	public ActionNode createSSHActionNode(String actionName, String host, String command, List<Arg> args,
			String okayNodeName, String errorNodeName) {
		ActionNode action = oozieNodeFactory.createActionNode();
		action.setName(actionName);
		SSH sshAction = oozieNodeFactory.createSSH();
		sshAction.setXmlns("uri:oozie:ssh-action:0.2");
		sshAction.setHost(host);
		sshAction.setCommand(command);
		Iterator<Arg> argsItr = args.iterator();
		Arg argument = null;
		while (argsItr.hasNext()) {
			argument = argsItr.next();
			sshAction.getArg().add(argument.getArg());
		}
		action.setSsh(sshAction);
		setOkTransition(action, okayNodeName);
		setErrorTransition(action, errorNodeName);
		return action;
	}

	/**
	 * @param act
	 * @param nodeName
	 */
	private void setOkTransition(ActionNode act, String nodeName) {

		if (act != null) {
			ActionTransition okCallWfTrans = oozieNodeFactory.createActionTransition();
			okCallWfTrans.setTo(nodeName);
			act.setOk(okCallWfTrans);
		}
	}

	/**
	 * This is for creating the error transition from one action to its error node
	 * @param act
	 * @param nodeName
	 */
	private void setErrorTransition(ActionNode act, String nodeName) {

		if (act != null) {
			ActionTransition errorCallWfTrans = oozieNodeFactory.createActionTransition();
			errorCallWfTrans.setTo(nodeName);
			act.setError(errorCallWfTrans);
		}
	}

	/**
	 * This is for creating the spark action node
	 * @param actionNodeName
	 * @param jobTracker
	 * @param nameNode
	 * @param master
	 * @param mode
	 * @param applicationName
	 * @param mainClass
	 * @param jarName
	 * @param args
	 * @param okayNodeName
	 * @param errorNodeName
	 * @return ActionNode
	 */
	public ActionNode createSparkActionNode(String actionNodeName, String jobTracker, String nameNode, String master,
			String mode, String applicationName, String mainClass, String jarName,
			List<Arg> args, String okayNodeName, String errorNodeName) {

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
		Iterator<Arg> argsItr = args.iterator();
		while (argsItr.hasNext()) {
			Arg arg = argsItr.next();
			sparkAction.getArg().add(arg.getArg());
		}

		setOkTransition(action, okayNodeName);
		setErrorTransition(action, errorNodeName);
		action.setSpark(sparkAction);
		return action;
	}

	/**
	 * For setting SparkOpts in spark action
	 * @return String
	 */
	private String setSparkOptions() {
		String sparkOpts = EMPTY_STRING;
		sparkOpts = "--queue " + "queue-name" + " --files " + "file-path" + " --driver-memory " + "driver-memory"
				+ " --executor-memory " + "executor-memory" + " --executor-cores " + "executor-cores";
		return sparkOpts;
	}
	
	public ActionNode createJavaActionNode(String actionNodeName, String jobTracker, String nameNode,
			PrepareNode prepareNode, String jobXml, List<ConfigurationProperties> configProperties,
			String mainClass, List<Arg> args, String okayNodeName, String errorNodeName) {
		ActionNode action = oozieNodeFactory.createActionNode();
		action.setName(actionNodeName);
		JavaAction javaAction = oozieNodeFactory.createJavaAction();
		javaAction.setJobTracker(jobTracker);
		javaAction.setNameNode(nameNode);
		Iterator<DeleteArg> deletArgsItr = prepareNode.getDeleteArgs().iterator();
		Prepare prepare = new Prepare();
		
		while (deletArgsItr.hasNext()) {
			DeleteArg arg = deletArgsItr.next();
			Delete delete = new Delete();
			delete.setPath(arg.getPath());
			prepare.getDelete().add(delete);
		}
		
		Iterator<MkdirArg> mkdirArgsItr = prepareNode.getMkdirArgs().iterator();
		while (mkdirArgsItr.hasNext()) {
			MkdirArg arg = mkdirArgsItr.next();
			Mkdir mkdir = new Mkdir();
			mkdir.setPath(arg.getPath());
			prepare.getMkdir().add(mkdir);
		}
		
		javaAction.setMainClass(mainClass);
		setOkTransition(action, okayNodeName);
		setErrorTransition(action, errorNodeName);
		action.setJava(javaAction);
		return action;
	}

}

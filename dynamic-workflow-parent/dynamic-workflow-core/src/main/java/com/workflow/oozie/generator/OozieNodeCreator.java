package com.workflow.oozie.generator;

import java.util.Iterator;
import java.util.List;

import com.workflow.oozie.model.Arg;
import com.workflow.oozie.model.ConfigurationProperties;
import com.workflow.oozie.model.DeleteArg;
import com.workflow.oozie.model.ForkPath;
import com.workflow.oozie.model.GlobalNodeDetails;
import com.workflow.oozie.model.MkdirArg;
import com.workflow.oozie.model.PrepareNode;
import com.workflow.oozie.nodes.ActionNode;
import com.workflow.oozie.nodes.ActionTransition;
import com.workflow.oozie.nodes.Configuration;
import com.workflow.oozie.nodes.Configuration.Property;
import com.workflow.oozie.nodes.Delete;
import com.workflow.oozie.nodes.End;
import com.workflow.oozie.nodes.Fork;
import com.workflow.oozie.nodes.ForkTransition;
import com.workflow.oozie.nodes.Global;
import com.workflow.oozie.nodes.JavaAction;
import com.workflow.oozie.nodes.Join;
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
		action.setOk(createActionTransition(okayNodeName));
		action.setError(createActionTransition(errorNodeName));
		return action;
	}

	/**
	 * This is for creating OkayTo and ErrorTo action transitions
	 * @param nodeName
	 */
	private ActionTransition createActionTransition(String nodeName) {
		ActionTransition actionTransition = oozieNodeFactory.createActionTransition();
		actionTransition.setTo(nodeName);
		return actionTransition;
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
		
		action.setOk(createActionTransition(okayNodeName));
		action.setError(createActionTransition(errorNodeName));
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
	
	/**
	 * This is for creating the Java action node
	 * @param actionNodeName
	 * @param jobTracker
	 * @param nameNode
	 * @param prepareNode
	 * @param jobXml
	 * @param configProperties
	 * @param mainClass
	 * @param args
	 * @param okayNodeName
	 * @param errorNodeName
	 * @return ActionNode
	 */
	public ActionNode createJavaActionNode(String actionNodeName, String jobTracker, String nameNode,
			PrepareNode prepareNode, String jobXml, List<ConfigurationProperties> configProperties,
			String mainClass, List<Arg> args, String okayNodeName, String errorNodeName) {
		
		ActionNode action = oozieNodeFactory.createActionNode();
		action.setName(actionNodeName);
		JavaAction javaAction = oozieNodeFactory.createJavaAction();
		
		Iterator<DeleteArg> deletArgsItr = prepareNode.getDeleteArgs().iterator();
		Prepare prepare = oozieNodeFactory.createPrepare();
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
		Iterator<ConfigurationProperties> configPropertiesItr = configProperties.iterator();
		Configuration config = oozieNodeFactory.createConfiguration();
		while (configPropertiesItr.hasNext()) {
			ConfigurationProperties property = configPropertiesItr.next();
			Property prop = oozieNodeFactory.createConfigurationProperty();
			prop.setName(property.getPropertyName());
			prop.setValue(property.getPropertyValue());
			config.getProperty().add(prop);
		}
		Iterator<Arg> argsItr = args.iterator();
		while (argsItr.hasNext()) {
			Arg arg = argsItr.next();
			javaAction.getArg().add(arg.getArg());
		}
		
		javaAction.setJobTracker(jobTracker);
		javaAction.setNameNode(nameNode);
		javaAction.setConfiguration(config);
		javaAction.setPrepare(prepare);
		javaAction.setMainClass(mainClass);
		action.setOk(createActionTransition(okayNodeName));
		action.setError(createActionTransition(errorNodeName));
		action.setJava(javaAction);
		return action;
	}
	
	/**
	 * This method is to create ForkActionNode
	 * @param forkNodeName
	 * @param forkPaths
	 * @return Fork
	 */
	public Fork createForkActionNode(String forkNodeName, List<ForkPath> forkPaths) {
		Fork fork = oozieNodeFactory.createFork();
		fork.setName(forkNodeName);
		Iterator<ForkPath> forkPathsItr = forkPaths.iterator();
		while (forkPathsItr.hasNext()) {
			ForkPath path = forkPathsItr.next();
			ForkTransition forkTransition = oozieNodeFactory.createForkTransition();
			forkTransition.setStart(path.getPath());
			fork.getPath().add(forkTransition);
		}
		return fork;
	}
	
	/**
	 * This is to create Join action node
	 * @param joinNodeName
	 * @param joinToName
	 * @return Join
	 */
	public Join createJoinActionNode(String joinNodeName, String joinToName) {
		Join join = oozieNodeFactory.createJoin();
		join.setName(joinNodeName);
		join.setTo(joinToName);
		return join;
	}

}

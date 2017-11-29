package com.workflow.oozie.nodes;


import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;

import javax.xml.namespace.QName;


public class OozieNodeFactory {
	
	private final static QName _WorkflowApp_QNAME = new QName("uri:oozie:workflow:0.4", "workflow-app");
	
	/*
	 * Create a new OozieNodeFactory that can be used to create new instances of
	 * schema derived classes for package: com.wf.business.oozieworkflow
	 * 
	 */
	
	public OozieNodeFactory() {
	}
	
	/**
	 * Create an instance of {@link GLOBAL }
	 * 
	 */
	public Global createGlobal() {
		return new Global();
	}
	
	/**
	 * Create an instance of {@link CREDENTIAL }
	 * 
	 */
	public Credential createCredential() {
		return new Credential();
	}

	/**
	 * Create an instance of {@link CONFIGURATION }
	 * 
	 */
	public Configuration createConfiguration() {
		return new Configuration();
	}

	/**
	 * Create an instance of {@link WorkFlowApp }
	 * 
	 */
	public WorkFlowApp createWorkFlowApp() {
		return new WorkFlowApp();
	}

	/**
	 * Create an instance of {@link FileSystem }
	 * 
	 */
	public FileSystem createFileSystem() {
		return new FileSystem();
	}

	/**
	 * Create an instance of {@link SubWorkFlow }
	 * 
	 */
	public SubWorkFlow createSubWorkFlow() {
		return new SubWorkFlow();
	}

	/**
	 * Create an instance of {@link KILL }
	 * 
	 */
	public Kill createKill() {
		return new Kill();
	}

	/**
	 * Create an instance of {@link JAVA }
	 * 
	 */
	public JavaAction createJavaAction() {
		return new JavaAction();
	}
	
	/**
	 * Create an instance of {@link SPARK }
	 * 
	 */
	public Spark createSpark() {
		return new Spark();
	}
	
	/**
	 * Create an instance of {@link SWITCH }
	 * 
	 */
	public Switch createSwitch() {
		return new Switch();
	}

	/**
	 * Create an instance of {@link MAPREDUCE }
	 * 
	 */
	public MapReduce createMapReduce() {
		return new MapReduce();
	}

	/**
	 * Create an instance of {@link PIG }
	 * 
	 */
	public Hive createHive() {
		return new Hive();
	}
	
	/**
     * Create an instance of {@link HIVE2 }
     * 
     */
    public Hive2 createHive2() {
        return new Hive2();
    }
    
	/**
	 * Create an instance of {@link STREAMING }
	 * 
	 */
	//	public STREAMING createSTREAMING() {
	//		return new STREAMING();
	//	}

	/**
	 * Create an instance of {@link PREPARE }
	 * 
	 */
	public Prepare createPrepare() {
		return new Prepare();
	}

	/**
	 * Create an instance of {@link DEFAULT }
	 * 
	 */
	public Default createDefault() {
		return new Default();
	}

	/**
	 * Create an instance of {@link CASE }
	 * 
	 */
	public Case createCase() {
		return new Case();
	}

	/**
	 * Create an instance of {@link PIPES }
	 * 
	 */
//	public PIPES createPIPES() {
//		return new PIPES();
//	}

	/**
	 * Create an instance of {@link DELETE }
	 * 
	 */
	public Delete createDelete() {
		return new Delete();
	}

	/**
	 * Create an instance of {@link JOIN }
	 * 
	 */
	public Join createJoin() {
		return new Join();
	}

	/**
	 * Create an instance of {@link ACTION }
	 * 
	 */
	public ActionNode createActionNode() {
		return new ActionNode();
	}

	/**
	 * Create an instance of {@link FLAG }
	 * 
	 */
	public Flag createFlag() {
		return new Flag();
	}

	/**
	 * Create an instance of {@link FORKTRANSITION }
	 * 
	 */
	public ForkTransition createForkTransition() {
		return new ForkTransition();
	}

	/**
	 * Create an instance of {@link ACTIONTRANSITION }
	 * 
	 */
	public ActionTransition createActionTransition() {
		return new ActionTransition();
	}

	/**
	 * Create an instance of {@link MKDIR }
	 * 
	 */
	public Mkdir createMkdir() {
		return new Mkdir();
	}

	    /**
	     * Create an instance of {@link SSH }
	     * 
	     */
	    public SSH createSSH() {
	        return new SSH();
	    }

	/**
	 * Create an instance of {@link FORK }
	 * 
	 */
	public Fork createFork() {
		return new Fork();
	}

	/**
	 * Create an instance of {@link CREDENTIALS }
	 * 
	 */
	public Credentials createCredentials() {
		return new Credentials();
	}

	/**
	 * Create an instance of {@link START }
	 * 
	 */
	public Start createStart() {
		return new Start();
	}

	/**
	 * Create an instance of {@link DECISION }
	 * 
	 */
	public Decision createDecision() {
		return new Decision();
	}

	/**
	 * Create an instance of {@link CHMOD }
	 * 
	 */
	public Chmod createChmod() {
		return new Chmod();
	}

	/**
	 * Create an instance of {@link END }
	 * 
	 */
	public End createEnd() {
		return new End();
	}

	/**
	 * Create an instance of {@link CREDENTIAL.Property }
	 * 
	 */
	public Credential.Property createCredentialProperty() {
		return new Credential.Property();
	}

	/**
	 * Create an instance of {@link CONFIGURATION.Property }
	 * 
	 */
	public Configuration.Property createConfigurationProperty() {
		return new Configuration.Property();
	}
	
	@XmlElementDecl(namespace = "uri:oozie:workflow:0.4", name = "workflow-app")
	public JAXBElement<WorkFlowApp> createWorkflowApp(WorkFlowApp value) {
		return new JAXBElement<WorkFlowApp>(_WorkflowApp_QNAME, WorkFlowApp.class, value);
	}

}

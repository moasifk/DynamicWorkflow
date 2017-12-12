# DynamicWorkflowGenerator
Dynamic workflow generator is a tool to generate oozie workflows dynamically based on an input json.
There are mainly 3 components for this.

1. Input json file
	A workflow is dynamically generated based on a json file. This json file generally decide the struture of the workflow.
	ie: it decide which all action nodes is required in the generated workflow and the flow of the action nodes from one action to another.
	The current json provided at  dynamic-workflow-services/resources/json.txt is basic framework for the input json which can be modified 
	as we required. Correspondgly we may need to change the DynamicWorkflowGeneratorImpl implementation.
	
2. Dynamic workflow core: dynamic-workflow-core
	
	2a. Generator
		This generator generated the workflow xml file.		
	2b. Nodes
		These are the POJO classes corresponding to each actions in a oozie workflow specification.
		Currently this is created based on the oozie workflow specification 3.0.
3. Dynamic workflow web services
	
	This provides a web service end for the workflow generation. A "/generateWorkflow" rest service is exposed,
	which will trigger the workflow generastion. This service accept the input json as argument. This is a POST
	service.

# Requirements
Java 1.7 or higher
Oozie 3.0.0
Maven 3.5.2

# Building workflow generator
mvn -DskipTests clean package

# Deploying war
You can run this application by deploying the generated war (dynamicWorkflowServices.war) in any of the webservers.


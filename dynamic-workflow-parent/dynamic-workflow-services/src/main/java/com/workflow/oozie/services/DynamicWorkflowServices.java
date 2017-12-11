package com.workflow.oozie.services;

import java.io.IOException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.workflow.oozie.generator.DynamicWorkflowGeneratorImpl;
import com.workflow.oozie.generator.WorkflowGenerator;
import com.workflow.oozie.model.DynamicWorkflowConfig;

@Path("/dynamicOozie")
public class DynamicWorkflowServices {

	Logger logger = Logger.getLogger(DynamicWorkflowServices.class);

	/*
	 * Webservice to handle the schedule request
	 */
	@POST
	@Path("/generateWorkflow")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response generateWorkflow(String requestJson) {
		logger.info("Started wfRestService:setSchedule");

		DynamicWorkflowConfig dynamicWorkflowConfigObj = null;
		String response = "";
		if (!requestJson.equals(null)) {
			Gson gson = new Gson();
			dynamicWorkflowConfigObj = gson.fromJson(requestJson, DynamicWorkflowConfig.class);
		}
		WorkflowGenerator workflowGenerator = new DynamicWorkflowGeneratorImpl();
		response = workflowGenerator.generateWorkFlow(dynamicWorkflowConfigObj);

		// String responseString = new Gson().toJson(responseJSON);
		// return HTTP response 201 in case of success
		return Response.status(Status.ACCEPTED).entity(response).build();
	}

}

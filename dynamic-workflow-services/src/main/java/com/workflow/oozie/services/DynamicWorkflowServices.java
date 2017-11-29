package com.workflow.oozie.services;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.workflow.oozie.generator.DynamicWorkflowGeneratorImpl;
import com.workflow.oozie.generator.WorkflowGenerator;
import com.workflow.oozie.model.DynamicWorkflowConfig;

/*
 *  Plain old Java Object it does not extend as class or implements
 *  an interface
 *  The class registers its methods for the HTTP POST request using the @POST annotation.
 *  Using the @Produces annotation, it defines that it can deliver several MIME types,
 *  text, XML and HTML.
 *  The browser requests per default the HTML MIME type. 
 */


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
		if (!requestJson.equals(null)) {
			Gson gson = new Gson();
			dynamicWorkflowConfigObj = gson.fromJson(
					requestJson, DynamicWorkflowConfig.class);
		}
		System.out.println(dynamicWorkflowConfigObj);
			WorkflowGenerator workflowGenerator = new DynamicWorkflowGeneratorImpl();
//			obj.generateWorkFlow(args);
//			responseJSON = wfManager.generateWorkFlow(requestStringBuilder.toString());
		
//		String responseString = new Gson().toJson(responseJSON);
		// return HTTP response 201 in case of success
		return Response.status(Status.ACCEPTED).entity("").build();
	}
	
} 

package com.workflow.oozie.services;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.Test;

/*
 *  Plain old Java Object it does not extend as class or implements
 *  an interface
 *  The class registers its methods for the HTTP POST request using the @POST annotation.
 *  Using the @Produces annotation, it defines that it can deliver several MIME types,
 *  text, XML and HTML.
 *  The browser requests per default the HTML MIME type. 
 */


public class TestDynamicWorkflowServices {
 
	Logger logger = Logger.getLogger(TestDynamicWorkflowServices.class);
	
	/*
	 * Webservice to handle the schedule request 
	 */
	/*
	 * @Test
	public void testSetSchedule() {
		logger.info("Started wfRestService:setSchedule");
		DynamicWorkflowServices obj = new DynamicWorkflowServices();
		StringBuilder stringFile = null;
		BufferedReader bufferedReader = null;
		try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader("E:\\MyWork\\dynamic-workflow-json\\json.txt");

            // Always wrap FileReader in BufferedReader.
            bufferedReader = 
                new BufferedReader(fileReader);
            stringFile = new StringBuilder();
            String line = "";
            while((line = bufferedReader.readLine()) != null) {
            	stringFile.append(line);
            }   

                   
        } catch (Exception e) {
        } finally {
        	// Always close files.
            try {
				bufferedReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
        }
		obj.generateWorkflow(stringFile.toString());
	}*/
	
} 

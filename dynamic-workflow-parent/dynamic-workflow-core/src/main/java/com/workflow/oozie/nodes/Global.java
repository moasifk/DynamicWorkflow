package com.workflow.oozie.nodes;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "global", propOrder = {
    "jobTracker",
    "nameNode",
    "configuration",
})

public class Global {
	   @XmlElement(name = "job-tracker", required = true)
	    protected String jobTracker;
	    @XmlElement(name = "name-node", required = true)
	    protected String nameNode;
	    protected Configuration configuration;

	    /**
	     * Gets the value of the jobTracker property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getJobTracker() {
	        return jobTracker;
	    }

	    /**
	     * Sets the value of the jobTracker property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setJobTracker(String value) {
	        this.jobTracker = value;
	    }

	    /**
	     * Gets the value of the nameNode property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getNameNode() {
	        return nameNode;
	    }

	    /**
	     * Sets the value of the nameNode property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setNameNode(String value) {
	        this.nameNode = value;
	    }

	    /**
	     * Gets the value of the configuration property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link Configuration }
	     *     
	     */
	    public Configuration getConfiguration() {
	        return configuration;
	    }

	    /**
	     * Sets the value of the configuration property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link CONFIGURATION }
	     *     
	     */
	    public void setConfiguration(Configuration value) {
	        this.configuration = value;
	    }
	}

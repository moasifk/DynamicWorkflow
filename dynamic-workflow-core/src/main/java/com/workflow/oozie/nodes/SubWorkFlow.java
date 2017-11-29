package com.workflow.oozie.nodes;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SUB-WORKFLOW", propOrder = {
    "appPath",
    "propagateConfiguration",
    "configuration"
})

public class SubWorkFlow {
	@XmlElement(name = "app-path", required = true)
    protected String appPath;
    @XmlElement(name = "propagate-configuration")
    protected Flag propagateConfiguration;
    protected Configuration configuration;

    /**
     * Gets the value of the appPath property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getAppPath() {
        return appPath;
    }

    /**
     * Sets the value of the appPath property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setAppPath(String value) {
        this.appPath = value;
    }

    /**
     * Gets the value of the propagateConfiguration property.
     * 
     * @return
     *     possible object is
     *     {@link Flag }
     *     
     */
    public Flag getPropagateConfiguration() {
        return propagateConfiguration;
    }

    /**
     * Sets the value of the propagateConfiguration property.
     * 
     * @param value
     *     allowed object is
     *     {@link Flag }
     *     
     */
    public void setPropagateConfiguration(Flag value) {
        this.propagateConfiguration = value;
    }

    /**
     * Gets the value of the configuration property.
     * 
     * @return
     *     possible object is
     *     {@link CONFIGURATION }
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


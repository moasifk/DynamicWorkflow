package com.workflow.oozie.nodes;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "MAP-REDUCE", propOrder = {
    "jobTracker",
    "nameNode",
    "mainClass",
    "prepare",
    "jobXml",
    "configuration",
    "file",
    "args",
    "archive"
})
	
public class MapReduce {

    @XmlElement(name = "job-tracker", required = true)
    protected String jobTracker;
    @XmlElement(name = "name-node", required = true)
    protected String nameNode;
    @XmlElement(name = "main-class")
    protected String mainClass;
    protected Prepare prepare;
    @XmlElement(name = "job-xml")
    protected String jobXml;
    protected Configuration configuration;
    protected List<String> file;
    protected List<String> args;
    protected List<String> archive;

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
     * Gets the value of the prepare property.
     * 
     * @return
     *     possible object is
     *     {@link PREPARE }
     *     
     */
    public Prepare getPrepare() {
        return prepare;
    }

    /**
     * Sets the value of the prepare property.
     * 
     * @param value
     *     allowed object is
     *     {@link Prepare }
     *     
     */
    public void setPrepare(Prepare value) {
        this.prepare = value;
    }

   
    /**
     * Gets the value of the jobXml property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getJobXml() {
        return jobXml;
    }

    /**
     * Sets the value of the jobXml property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setJobXml(String value) {
        this.jobXml = value;
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
     *     {@link Configuration }
     *     
     */
    public void setConfiguration(Configuration value) {
        this.configuration = value;
    }

    /**
     * Gets the value of the file property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the file property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getFile().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getFile() {
        if (file == null) {
            file = new ArrayList<String>();
        }
        return this.file;
    }
    /**
     * Gets the value of the mainClass property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMainClass() {
        return mainClass;
    }

    /**
     * Sets the value of the mainClass property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMainClass(String value) {
        this.mainClass = value;
    }

    /**
     * Gets the value of the archive property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the archive property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getArchive().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getArchive() {
        if (archive == null) {
            archive = new ArrayList<String>();
        }
        return this.archive;
    }
    
    public List<String> getArgs() {
        if (args == null) {
        	args = new ArrayList<String>();
        }
        return this.args;
    }
}

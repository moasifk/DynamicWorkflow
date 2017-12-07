package com.workflow.oozie.nodes;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Action", propOrder = {
    "mapReduce",
    "ssh",
    "hive",
    "subWorkflow",
    "fs",
     "java",
     "spark",
     "hive2",
    "ok",
    "error",
    "any"
})

public class ActionNode {
	    @XmlElement(name = "map-reduce")
	    protected MapReduce mapReduce;
	    
	    protected Hive hive;
	    protected Hive2 hive2;
	    
	    @XmlElement(name = "sub-workflow")
	    protected SubWorkFlow subWorkflow;
	    
	    protected FileSystem fs;
	    protected JavaAction java;
	    protected Spark spark;
	    protected SSH ssh;
	    
	    @XmlElement(required = true)
	    protected ActionTransition ok;
	    
	    @XmlElement(required = true)
	    protected ActionTransition error;
	    
	    @XmlAnyElement(lax = true)
	    protected Object any;
	    
	    @XmlAttribute(name = "name", required = true)
	    protected String name;
	    @XmlAttribute(name = "cred")
	    protected String cred;
	    @XmlAttribute(name = "retry-max")
	    protected String retryMax;
	    @XmlAttribute(name = "retry-interval")
	    protected String retryInterval;

	    /**
	     * Gets the value of the mapReduce property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link MapReduce }
	     *     
	     */
	    public MapReduce getMapReduce() {
	        return mapReduce;
	    }

	    /**
	     * Sets the value of the mapReduce property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link MapReduce }
	     *     
	     */
	    public void setMapReduce(MapReduce value) {
	        this.mapReduce = value;
	    }

	    /**
	     * Gets the value of the hive property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link hive }
	     *     
	     */
	    public Hive getHive() {
	        return hive;
	    }

	    /**
	     * Sets the value of the hive property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link Hive }
	     *     
	     */
	    public void setHive(Hive value) {
	        this.hive = value;
	    }

	    /**
	     * Gets the value of the subWorkflow property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link SUBWORKFLOW }
	     *     
	     */
	    public SubWorkFlow getSubWorkflow() {
	        return subWorkflow;
	    }

	    /**
	     * Sets the value of the subWorkflow property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link SubWorkFlow }
	     *     
	     */
	    public void setSubWorkflow(SubWorkFlow value) {
	        this.subWorkflow = value;
	    }

	    /**
	     * Gets the value of the fs property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link FileSystem }
	     *     
	     */
	    public FileSystem getFs() {
	        return fs;
	    }

	    /**
	     * Sets the value of the fs property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link FileSystem }
	     *     
	     */
	    public void setFs(FileSystem value) {
	        this.fs = value;
	    }

	    /**
	     * Gets the value of the JavaAction property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link JavaAction }
	     *     
	     */
	    public JavaAction getJava() {
	        return java;
	    }

	    /**
	     * Sets the value of the JavaAction property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link JavaAction }
	     *     
	     */
	    public void setJava(JavaAction value) {
	        this.java = value;
	    }

	    /**
	     * Gets the value of the ok property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link ActionTransition }
	     *     
	     */
	    public ActionTransition getOk() {
	        return ok;
	    }

	    /**
	     * Sets the value of the ok property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link ActionTransition }
	     *     
	     */
	    public void setOk(ActionTransition value) {
	        this.ok = value;
	    }

	    /**
	     * Gets the value of the error property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link ActionTransition }
	     *     
	     */
	    public ActionTransition getError() {
	        return error;
	    }

	    /**
	     * Sets the value of the error property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link ActionTransition }
	     *     
	     */
	    public void setError(ActionTransition value) {
	        this.error = value;
	    }

	    /**
	     * Gets the value of the any property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link Object }
	     *     
	     */
	    public Object getAny() {
	        return any;
	    }

	    /**
	     * Sets the value of the any property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link Object }
	     *     
	     */
	    public void setAny(Object value) {
	        this.any = value;
	    }

	    /**
	     * Gets the value of the name property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getName() {
	        return name;
	    }

	    /**
	     * Sets the value of the name property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setName(String value) {
	        this.name = value;	//Limiting node name to max 50 char.
	    }

	    /**
	     * Gets the value of the cred property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getCred() {
	        return cred;
	    }

	    /**
	     * Sets the value of the cred property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setCred(String value) {
	        this.cred = value;
	    }

	    /**
	     * Gets the value of the retryMax property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getRetryMax() {
	        return retryMax;
	    }

	    /**
	     * Sets the value of the retryMax property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setRetryMax(String value) {
	        this.retryMax = value;
	    }

	    /**
	     * Gets the value of the retryInterval property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getRetryInterval() {
	        return retryInterval;
	    }

	    /**
	     * Sets the value of the retryInterval property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setRetryInterval(String value) {
	        this.retryInterval = value;
	    }

	    public SSH getSsh() {
	        return ssh;
	    }

	    public void setSsh(SSH ssh) {
	        this.ssh = ssh;
	    }

	    public Spark getSpark() {
	        return spark;
	    }

	    public void setSpark(Spark spark) {
	        this.spark = spark;
	    }

		public Hive2 getHive2() {
			return hive2;
		}

		public void setHive2(Hive2 hive2) {
			this.hive2 = hive2;
		}

}


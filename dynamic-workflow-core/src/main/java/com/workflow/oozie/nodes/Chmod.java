package com.workflow.oozie.nodes;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "chmod")
public class Chmod {
	   @XmlAttribute(name = "path", required = true)
	    protected String path;
	    @XmlAttribute(name = "permissions", required = true)
	    protected String permissions;
	    @XmlAttribute(name = "dir-files")
	    protected String dirFiles;

	    /**
	     * Gets the value of the path property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getPath() {
	        return path;
	    }

	    /**
	     * Sets the value of the path property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setPath(String value) {
	        this.path = value;
	    }

	    /**
	     * Gets the value of the permissions property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getPermissions() {
	        return permissions;
	    }

	    /**
	     * Sets the value of the permissions property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setPermissions(String value) {
	        this.permissions = value;
	    }

	    /**
	     * Gets the value of the dirFiles property.
	     * 
	     * @return
	     *     possible object is
	     *     {@link String }
	     *     
	     */
	    public String getDirFiles() {
	        return dirFiles;
	    }

	    /**
	     * Sets the value of the dirFiles property.
	     * 
	     * @param value
	     *     allowed object is
	     *     {@link String }
	     *     
	     */
	    public void setDirFiles(String value) {
	        this.dirFiles = value;
	    }

	}

package com.workflow.oozie.nodes;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SSH", propOrder = {
    "host",
    "command",
    "arg",
    "captureOutput"
})
public class SSH {

    @XmlElement(required = true)
    protected String host;
    @XmlElement(required = true)
    protected String command;
    protected List<String> arg;
    @XmlElement(name = "capture-output")
    protected Flag captureOutput;
    
    @XmlAttribute(name = "xmlns", required = true)
    protected String xmlns;
    
    /**
     * Gets the value of the host property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the value of the host property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setHost(String value) {
        this.host = value;
    }

    /**
     * Gets the value of the command property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCommand() {
        return command;
    }

    /**
     * Sets the value of the command property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCommand(String value) {
        this.command = value;
    }

    /**
     * Gets the value of the arg property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the arg property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getArg().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getArg() {
        if (arg == null) {
            arg = new ArrayList<String>();
        }
        return this.arg;
    }

    /**
     * Gets the value of the captureOutput property.
     * 
     * @return
     *     possible object is
     *     {@link FLAG }
     *     
     */
    public Flag getCaptureOutput() {
        return captureOutput;
    }

    /**
     * Sets the value of the captureOutput property.
     * 
     * @param value
     *     allowed object is
     *     {@link FLAG }
     *     
     */
    public void setCaptureOutput(Flag value) {
        this.captureOutput = value;
    }

    public String getXmlns() {
        return xmlns;
    }

    public void setXmlns(String xmlns) {
        this.xmlns = xmlns;
    }

}

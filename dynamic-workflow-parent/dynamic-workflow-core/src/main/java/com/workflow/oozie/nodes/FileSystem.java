package com.workflow.oozie.nodes;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "fs", propOrder = {
    "delete",
    "mkdir",
    "move",
    "chmod"
})

public class FileSystem {
	    protected List<Delete> delete;
	    protected List<Mkdir> mkdir;
	    protected List<Move> move;
	    protected List<Chmod> chmod;

	    /**
	     * Gets the value of the delete property.
	     * 
	     * <p>
	     * This accessor method returns a reference to the live list,
	     * not a snapshot. Therefore any modification you make to the
	     * returned list will be present inside the JAXB object.
	     * This is why there is not a <CODE>set</CODE> method for the delete property.
	     * 
	     * <p>
	     * For example, to add a new item, do as follows:
	     * <pre>
	     *    getDelete().add(newItem);
	     * </pre>
	     * 
	     * 
	     * <p>
	     * Objects of the following type(s) are allowed in the list
	     * {@link DELETE }
	     * 
	     * 
	     */
	    public List<Delete> getDelete() {
	        if (delete == null) {
	            delete = new ArrayList<Delete>();
	        }
	        return this.delete;
	    }

	    /**
	     * Gets the value of the mkdir property.
	     * 
	     * <p>
	     * This accessor method returns a reference to the live list,
	     * not a snapshot. Therefore any modification you make to the
	     * returned list will be present inside the JAXB object.
	     * This is why there is not a <CODE>set</CODE> method for the mkdir property.
	     * 
	     * <p>
	     * For example, to add a new item, do as follows:
	     * <pre>
	     *    getMkdir().add(newItem);
	     * </pre>
	     * 
	     * 
	     * <p>
	     * Objects of the following type(s) are allowed in the list
	     * {@link MKDIR }
	     * 
	     * 
	     */
	    public List<Mkdir> getMkdir() {
	        if (mkdir == null) {
	            mkdir = new ArrayList<Mkdir>();
	        }
	        return this.mkdir;
	    }

	    /**
	     * Gets the value of the move property.
	     * 
	     * <p>
	     * This accessor method returns a reference to the live list,
	     * not a snapshot. Therefore any modification you make to the
	     * returned list will be present inside the JAXB object.
	     * This is why there is not a <CODE>set</CODE> method for the move property.
	     * 
	     * <p>
	     * For example, to add a new item, do as follows:
	     * <pre>
	     *    getMove().add(newItem);
	     * </pre>
	     * 
	     * 
	     * <p>
	     * Objects of the following type(s) are allowed in the list
	     * {@link MOVE }
	     * 
	     * 
	     */
	    public List<Move> getMove() {
	        if (move == null) {
	            move = new ArrayList<Move>();
	        }
	        return this.move;
	    }

	    /**
	     * Gets the value of the chmod property.
	     * 
	     * <p>
	     * This accessor method returns a reference to the live list,
	     * not a snapshot. Therefore any modification you make to the
	     * returned list will be present inside the JAXB object.
	     * This is why there is not a <CODE>set</CODE> method for the chmod property.
	     * 
	     * <p>
	     * For example, to add a new item, do as follows:
	     * <pre>
	     *    getChmod().add(newItem);
	     * </pre>
	     * 
	     * 
	     * <p>
	     * Objects of the following type(s) are allowed in the list
	     * {@link CHMOD }
	     * 
	     * 
	     */
	    public List<Chmod> getChmod() {
	        if (chmod == null) {
	            chmod = new ArrayList<Chmod>();
	        }
	        return this.chmod;
	    }

	}

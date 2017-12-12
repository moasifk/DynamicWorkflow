
package com.workflow.oozie.model;

import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class SparkNodeDetails {

    @SerializedName("NodeName")
    @Expose
    private String nodeName;
    @SerializedName("JobTracker")
    @Expose
    private String jobTracker;
    @SerializedName("NameNode")
    @Expose
    private String nameNode;
    @SerializedName("Master")
    @Expose
    private String master;
    @SerializedName("Mode")
    @Expose
    private String mode;
    @SerializedName("ApplicationName")
    @Expose
    private String applicationName;
    @SerializedName("Jar")
    @Expose
    private String jar;
    @SerializedName("SparkOpts")
    @Expose
    private String sparkOpts;
    @SerializedName("Args")
    @Expose
    private List<Arg> args = new ArrayList<Arg>();
    @SerializedName("OkayToName")
    @Expose
    private String okayToName;
    @SerializedName("ErrorToName")
    @Expose
    private String errorToName;
    @SerializedName("ClassName")
    @Expose
    private String className;

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getJobTracker() {
        return jobTracker;
    }

    public void setJobTracker(String jobTracker) {
        this.jobTracker = jobTracker;
    }

    public String getNameNode() {
        return nameNode;
    }

    public void setNameNode(String nameNode) {
        this.nameNode = nameNode;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getJar() {
        return jar;
    }

    public void setJar(String jar) {
        this.jar = jar;
    }

    public String getSparkOpts() {
        return sparkOpts;
    }

    public void setSparkOpts(String sparkOpts) {
        this.sparkOpts = sparkOpts;
    }

    public List<Arg> getArgs() {
        return args;
    }

    public void setArgs(List<Arg> args) {
        this.args = args;
    }

    public String getOkayToName() {
        return okayToName;
    }

    public void setOkayToName(String okayToName) {
        this.okayToName = okayToName;
    }

    public String getErrorToName() {
        return errorToName;
    }

    public void setErrorToName(String errorToName) {
        this.errorToName = errorToName;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("nodeName", nodeName).append("jobTracker", jobTracker).append("nameNode", nameNode).append("master", master).append("mode", mode).append("name", applicationName).append("jar", jar).append("sparkOpts", sparkOpts).append("args", args).append("okayToName", okayToName).append("errorToName", errorToName).toString();
    }

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

}

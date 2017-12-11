
package com.workflow.oozie.model;

import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class GlobalNodeDetails {

    @SerializedName("GlobalNodeName")
    @Expose
    private String globalNodeName;
    @SerializedName("JobTracker")
    @Expose
    private String jobTracker;
    @SerializedName("NameNode")
    @Expose
    private String nameNode;
    @SerializedName("ConfigurationProperties")
    @Expose
    private List<ConfigurationProperties> properties = new ArrayList<ConfigurationProperties>();

    public String getGlobalNodeName() {
        return globalNodeName;
    }

    public void setGlobalNodeName(String globalNodeName) {
        this.globalNodeName = globalNodeName;
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

    public List<ConfigurationProperties> getConfigProperties() {
        return properties;
    }

    public void setConfigProperties(List<ConfigurationProperties> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("globalNodeName", globalNodeName).append("jobTracker", jobTracker).append("nameNode", nameNode).append("properties", properties).toString();
    }

}

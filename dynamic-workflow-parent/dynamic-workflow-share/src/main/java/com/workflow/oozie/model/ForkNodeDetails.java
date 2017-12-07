
package com.workflow.oozie.model;

import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ForkNodeDetails {

    @SerializedName("NodeName")
    @Expose
    private String nodeName;
    @SerializedName("ForkPaths")
    @Expose
    private List<ForkPath> forkPaths = new ArrayList<ForkPath>();

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public List<ForkPath> getForkPaths() {
        return forkPaths;
    }

    public void setForkPaths(List<ForkPath> forkPaths) {
        this.forkPaths = forkPaths;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("nodeName", nodeName).append("forkPaths", forkPaths).toString();
    }

}

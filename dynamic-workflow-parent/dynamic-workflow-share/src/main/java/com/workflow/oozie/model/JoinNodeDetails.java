
package com.workflow.oozie.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class JoinNodeDetails {

    @SerializedName("NodeName")
    @Expose
    private String nodeName;
    @SerializedName("JoinToName")
    @Expose
    private String joinToName;

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

	public String getJoinToName() {
		return joinToName;
	}

	public void setJoinToName(String joinToName) {
		this.joinToName = joinToName;
	}
}

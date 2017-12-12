
package com.workflow.oozie.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class KillNodeDetails {

    @SerializedName("KillNodeMessage")
    @Expose
    private String killNodeMessage;
    
    @SerializedName("KillNodeName")
    @Expose
    private String killNodeName;


	public String getKillNodeName() {
		return killNodeName;
	}

	public void setKillNodeName(String killNodeName) {
		this.killNodeName = killNodeName;
	}

	public String getKillNodeMessage() {
		return killNodeMessage;
	}

	public void setKillNodeMessage(String killNodeMessage) {
		this.killNodeMessage = killNodeMessage;
	}


}

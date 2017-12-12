
package com.workflow.oozie.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class DynamicWorkflowConfig {
	
	@SerializedName("WorkflowName")
    @Expose
    private String workflowName;
	
    @SerializedName("StartNodeName")
    @Expose
    private String startNodeName;
    @SerializedName("GlobalNodeDetails")
    @Expose
    private GlobalNodeDetails globalNodeDetails;
    @SerializedName("SSHNodeDetails")
    @Expose
    private SSHNodeDetails sSHNodeDetails;
    @SerializedName("ForkNodeDetails")
    @Expose
    private ForkNodeDetails forkNodeDetails;
    
    @SerializedName("JoinNodeDetails")
    @Expose
    private JoinNodeDetails joinNodeDetails;
    
    @SerializedName("SparkNodeDetails")
    @Expose
    private SparkNodeDetails sparkNodeDetails;
    
    @SerializedName("JavaNodeDetails")
    @Expose
    private JavaNodeDetails javaNodeDetails;
    
    @SerializedName("EndNodeName")
    @Expose
    private String endNodeName;
    
    @SerializedName("KillNodeDetails")
    @Expose
    private KillNodeDetails killNodeDetails;
    
    public String getStartNodeName() {
        return startNodeName;
    }

    public void setStartNodeName(String startNodeName) {
        this.startNodeName = startNodeName;
    }

    public GlobalNodeDetails getGlobalNodeDetails() {
        return globalNodeDetails;
    }

    public void setGlobalNodeDetails(GlobalNodeDetails globalNodeDetails) {
        this.globalNodeDetails = globalNodeDetails;
    }

    public SSHNodeDetails getSSHNodeDetails() {
        return sSHNodeDetails;
    }

    public void setSSHNodeDetails(SSHNodeDetails sSHNodeDetails) {
        this.sSHNodeDetails = sSHNodeDetails;
    }

    public ForkNodeDetails getForkNodeDetails() {
        return forkNodeDetails;
    }

    public void setForkNodeDetails(ForkNodeDetails forkNodeDetails) {
        this.forkNodeDetails = forkNodeDetails;
    }

    public SparkNodeDetails getSparkNodeDetails() {
        return sparkNodeDetails;
    }

    public void setSparkNodeDetails(SparkNodeDetails sparkNodeDetails) {
        this.sparkNodeDetails = sparkNodeDetails;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("startNodeName", startNodeName).append("globalNodeDetails", globalNodeDetails).append("sSHNodeDetails", sSHNodeDetails).append("forkNodeDetails", forkNodeDetails).append("sparkNodeDetails", sparkNodeDetails).toString();
    }

	public String getEndNodeName() {
		return endNodeName;
	}

	public void setEndNodeName(String endNodeName) {
		this.endNodeName = endNodeName;
	}

	public KillNodeDetails getKillNodeDetails() {
		return killNodeDetails;
	}

	public void setKillNodeDetails(KillNodeDetails killNodeDetails) {
		this.killNodeDetails = killNodeDetails;
	}

	public String getWorkflowName() {
		return workflowName;
	}

	public void setWorkflowName(String workflowName) {
		this.workflowName = workflowName;
	}

	public JavaNodeDetails getJavaNodeDetails() {
		return javaNodeDetails;
	}

	public void setJavaNodeDetails(JavaNodeDetails javaNodeDetails) {
		this.javaNodeDetails = javaNodeDetails;
	}

	public JoinNodeDetails getJoinNodeDetails() {
		return joinNodeDetails;
	}

	public void setJoinNodeDetails(JoinNodeDetails joinNodeDetails) {
		this.joinNodeDetails = joinNodeDetails;
	}

}

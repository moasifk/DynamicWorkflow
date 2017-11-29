
package com.workflow.oozie.model;

import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class SSHNodeDetails {

    @SerializedName("NodeName")
    @Expose
    private String nodeName;
    @SerializedName("Host")
    @Expose
    private String host;
    @SerializedName("Command")
    @Expose
    private String command;
    @SerializedName("Args")
    @Expose
    private List<Arg> args = new ArrayList<Arg>();

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public List<Arg> getArgs() {
        return args;
    }

    public void setArgs(List<Arg> args) {
        this.args = args;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("nodeName", nodeName).append("host", host).append("command", command).append("args", args).toString();
    }

}
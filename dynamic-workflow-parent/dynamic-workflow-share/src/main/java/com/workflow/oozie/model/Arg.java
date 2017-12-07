
package com.workflow.oozie.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class Arg {

    @SerializedName("Arg")
    @Expose
    private String arg;

    public String getArg() {
        return arg;
    }

    public void setArg(String arg) {
        this.arg = arg;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("arg", arg).toString();
    }

}

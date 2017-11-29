
package com.workflow.oozie.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ForkPath {

    @SerializedName("Path")
    @Expose
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("path", path).toString();
    }

}

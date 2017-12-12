
package com.workflow.oozie.model;

import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class PrepareNode {

    @SerializedName("DeleteArgs")
    @Expose
    private List<DeleteArg> deleteArgs = new ArrayList<DeleteArg>();
    @SerializedName("MkdirArgs")
    @Expose
    private List<MkdirArg> mkdirArgs = new ArrayList<MkdirArg>();

    public List<DeleteArg> getDeleteArgs() {
        return deleteArgs;
    }

    public void setDeleteArgs(List<DeleteArg> deleteArgs) {
        this.deleteArgs = deleteArgs;
    }

    public List<MkdirArg> getMkdirArgs() {
        return mkdirArgs;
    }

    public void setMkdirArgs(List<MkdirArg> mkdirArgs) {
        this.mkdirArgs = mkdirArgs;
    }

}

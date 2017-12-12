
package com.workflow.oozie.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ConfigurationProperties {

    @SerializedName("PropertyName")
    @Expose
    private String propertyName;
    @SerializedName("PropertyValue")
    @Expose
    private String propertyValue;

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyValue() {
        return propertyValue;
    }

    public void setPropertyValue(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("propertyName", propertyName).append("propertyValue", propertyValue).toString();
    }

}

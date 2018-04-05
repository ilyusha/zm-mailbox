package com.zimbra.cs.ml.schema;

import com.zimbra.cs.ml.Classification;

/**
 * The result of classifying an email
 */
public class ClassificationResult extends Classification {

    private String textUrl;


    public String getUrl() {
        return textUrl;
    }
    public void setUrl(String textUrl) {
        this.textUrl = textUrl;
    }
}

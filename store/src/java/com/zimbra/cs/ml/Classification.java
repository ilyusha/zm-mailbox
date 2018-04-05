package com.zimbra.cs.ml;

import com.zimbra.cs.ml.schema.OverlappingClassification;

public abstract class Classification {

    private String exclusiveClass;
    private OverlappingClassification[] overlappingClasses;

    public OverlappingClassification[] getOverlappingClasses() {
        return overlappingClasses;
    }

    public void setOverlappingClasses(OverlappingClassification[] overlappingClasses) {
        this.overlappingClasses = overlappingClasses;
    }

    public String getExclusiveClass() {
        return exclusiveClass;
    }

    public void setExclusiveClass(String exclusiveClass) {
        this.exclusiveClass = exclusiveClass;
    }
}

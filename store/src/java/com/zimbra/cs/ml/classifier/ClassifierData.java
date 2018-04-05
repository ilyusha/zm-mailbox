package com.zimbra.cs.ml.classifier;

import com.zimbra.cs.ml.Classifiable;
import com.zimbra.cs.ml.classifier.AbstractClassifier.ClassifierConfig;
import com.zimbra.cs.ml.feature.FeatureSet;
import com.zimbra.cs.ml.schema.ClassifierSpec;

/**
 * Class encompassing all data needed to register a Zimbra classifier
 */
public class ClassifierData<T extends Classifiable> {
    private ClassifierConfig metadata;
    private FeatureSet<T> featureSet;
    private ClassifierSpec spec;

    public ClassifierData(ClassifierConfig metadata, ClassifierSpec spec, FeatureSet<T> featureSet) {
        this.metadata = metadata;
        this.featureSet = featureSet;
        this.spec = spec;
    }

    public ClassifierConfig getMetadata() {
        return metadata;
    }

    public ClassifierSpec getSpec() {
        return spec;
    }

    public FeatureSet<T> getFeatureSet() {
        return featureSet;
    }
}
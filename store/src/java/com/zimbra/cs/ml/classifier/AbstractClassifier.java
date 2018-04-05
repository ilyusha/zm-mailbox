package com.zimbra.cs.ml.classifier;

import java.util.List;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.ml.Classifiable;
import com.zimbra.cs.ml.Classification;
import com.zimbra.cs.ml.feature.ComputedFeatures;
import com.zimbra.cs.ml.feature.FeatureSet;
import com.zimbra.cs.ml.feature.FeatureSpec.EncodedFeatureSpecs;

public abstract class AbstractClassifier<T extends Classifiable, R extends Classification> {

    private String id;
    private FeatureSet<T> featureSet;
    private ClassifierConfig config;

    //Private constructor because retrieving a Classifier instance should be done
    //through the factory, while registering a new one is handled by subclass-specific
    //static methods
    protected AbstractClassifier(String id, ClassifierConfig config, FeatureSet<T> featureSet) {
        this.id = id;
        this.config = config;
        this.featureSet = featureSet;
    }

    public String getLabel() {
        return config.label;
    }


    public FeatureSet<T> getFeatureSet() {
        return featureSet;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return config.description;
    }

    /**
     * Return a string that corresponds to the key that this Classifier's factory was registered with.
     */
    public abstract String getClassifierType();

    /**
     * Return a string that corresponds to the key that this Classifier's FeatureSet factory was registered with.
     */
    public abstract String getFeatureSetType();


    public String[] getOverlappingClasses() {
        return config.overlappingClasses;
    }

    public String[] getExclusiveClasses() {
        return config.exclusiveClasses;
    }

    protected abstract R classifyFeatures(ComputedFeatures<T> features) throws ServiceException;

    public R classifyItem(T item) throws ServiceException {
        return classifyFeatures(getFeatureSet().getFeatures(item));
    }

    public static class ClassifierConfig {
        private String label;
        private String description;
        private String[] exclusiveClasses;
        private String[] overlappingClasses;

        public ClassifierConfig(String label, String description, List<String> exclusive, List<String> overlapping) {
            this(label, description,
                    exclusive.toArray(new String[exclusive.size()]),
                    overlapping.toArray(new String[overlapping.size()]));
        }

        public ClassifierConfig(String label, String description, String[] exclusive, String[] overlapping) {
            this.label = label;
            this.description = description;
            this.exclusiveClasses = exclusive;
            this.overlappingClasses = overlapping;
        }

        public String getLabel() { return label; }
    }

    public static abstract class Factory<T extends Classifiable, R extends Classification> {

        @SuppressWarnings("unchecked")
        public AbstractClassifier<T, R> getClassifier(String id, ClassifierConfig config, EncodedFeatureSpecs encodedFeatureSpecs) throws ServiceException {
            String featureSetType = encodedFeatureSpecs.getType();
            FeatureSet.Factory<T> featureSetFactory = (FeatureSet.Factory<T>) ClassifierManager.getFeatureSetFactory(featureSetType);
            FeatureSet<T> featureSet = featureSetFactory.buildFeatureSet(encodedFeatureSpecs.getEncodedSpecs());
            return getClassifier(id, config, featureSet);
        }

        protected abstract AbstractClassifier<T, R> getClassifier(String id, ClassifierConfig config, FeatureSet<T> featureSet) throws ServiceException;
    }
}

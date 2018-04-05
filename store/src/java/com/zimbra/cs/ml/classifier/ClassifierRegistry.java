package com.zimbra.cs.ml.classifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.BEncoding;
import com.zimbra.common.util.BEncoding.BEncodingException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.ml.Classifiable;
import com.zimbra.cs.ml.Classification;
import com.zimbra.cs.ml.classifier.AbstractClassifier.ClassifierConfig;
import com.zimbra.cs.ml.feature.FeatureSpec;
import com.zimbra.cs.ml.feature.FeatureSpec.EncodedFeatureSpecs;

/**
 * Registry of classifiers known to be initialized on the ML server.
 * Subclasses handle persisting/loading classifier info.
 */
public abstract class ClassifierRegistry {

    private static final String KEY_ID = "i";
    private static final String KEY_LABEL = "l";
    private static final String KEY_FEATURES = "f";
    private static final String KEY_DESCRIPTION = "d";
    private static final String KEY_CLASSIFIER_TYPE = "t";
    private static final String KEY_CLASSIFIABLE_TYPE = "c";
    private static final String KEY_OVERLAPPING_CLASSES = "oc";
    private static final String KEY_EXCLUSIVE_CLASSES = "ec";

    private Map<String, AbstractClassifier<?,?>> idMap;
    private Map<String, AbstractClassifier<?,?>> labelMap;
    private boolean loaded = false;

    public ClassifierRegistry() {
        idMap = new HashMap<>();
        labelMap = new HashMap<>();
    }

    private synchronized void loadKnownClassifiers() throws ServiceException {
        String[] encodedClassifiers = load();
        ZimbraLog.ml.info("found known %d classifiers in %s", encodedClassifiers.length, this.getClass().getSimpleName());
        for (String encoded: encodedClassifiers) {
            AbstractClassifier<?, ?> decoded = decode(encoded);
            String id = decoded.getId();
            idMap.put(id, decoded);
            labelMap.put(decoded.getLabel(), decoded);
        }
        loaded = true;
    }

    public Map<String, AbstractClassifier<?,?>> getAllClassifiers() throws ServiceException {
        if (!loaded) {
            loadKnownClassifiers();
        }
        return idMap;
    }

    public boolean labelExists(String label) throws ServiceException {
        if (!loaded) {
            loadKnownClassifiers();
        }
        return labelMap.containsKey(label);
    }

    /**
     * return known encoded classifiers
     */
    protected abstract String[] load() throws ServiceException;

    /**
     * Persist the encoded classifier string
     */
    protected abstract void save(String encodedClassifier) throws ServiceException;

    /**
     * Register a classifier
     */
    public void register(AbstractClassifier<?,?> classifier) throws ServiceException {
        if (!loaded) {
            loadKnownClassifiers();
        }
        if (labelExists(classifier.getLabel())) {
            throw ServiceException.FAILURE("label '" + classifier.getLabel() + "' already exists", null);
        }
        idMap.put(classifier.getId(), classifier);
        labelMap.put(classifier.getLabel(), classifier);
        save(encode(classifier));
    }

    protected String encode(AbstractClassifier<?,?> classifier) {
        Map<String, Object> map = new HashMap<>();
        map.put(KEY_ID, classifier.getId());
        map.put(KEY_CLASSIFIER_TYPE, classifier.getClassifierType());
        map.put(KEY_CLASSIFIABLE_TYPE, classifier.getFeatureSetType());
        map.put(KEY_LABEL, classifier.getLabel());
        map.put(KEY_FEATURES, classifier.getFeatureSet().getAllFeatureSpecs().stream().map(feature -> ((FeatureSpec<?>) feature).encode()).collect(Collectors.toList()));
        map.put(KEY_DESCRIPTION, classifier.getDescription());
        map.put(KEY_EXCLUSIVE_CLASSES, Arrays.asList(classifier.getExclusiveClasses()));
        map.put(KEY_OVERLAPPING_CLASSES, Arrays.asList(classifier.getOverlappingClasses()));
        return BEncoding.encode(map);
    }

    @SuppressWarnings("unchecked")
    protected AbstractClassifier<?, ?> decode(String encoded) throws ServiceException {
        Map<String, Object> map;
        try {
            map = BEncoding.decode(encoded);
        } catch (BEncodingException e) {
            throw ServiceException.FAILURE("unable to decode classifier with encoded value %s" + encoded, null);
        }
        if (!map.containsKey(KEY_CLASSIFIER_TYPE)) {
            throw ServiceException.FAILURE("no classifier type value found during decoding classifier", null);
        }
        String classifierType = (String) map.get(KEY_CLASSIFIER_TYPE);
        String label = (String) map.get(KEY_LABEL);
        String id = (String) map.get(KEY_ID);
        String description = map.containsKey(KEY_DESCRIPTION) ? (String) map.get(KEY_DESCRIPTION) : null;
        String featureSetType = (String) map.get(KEY_CLASSIFIABLE_TYPE);
        List<String> overlappingClasses = (List<String>) map.get(KEY_OVERLAPPING_CLASSES);
        List<String> exclusiveClasses = (List<String>) map.get(KEY_EXCLUSIVE_CLASSES);
        List<String> encodedFeatures = (List<String>) map.get(KEY_FEATURES);
        EncodedFeatureSpecs encodedFeatureSpecs = new EncodedFeatureSpecs(featureSetType, encodedFeatures);
        AbstractClassifier.Factory<?, ?> classifierFactory = ClassifierManager.getClassifierFactory(classifierType);
        ClassifierConfig config = new ClassifierConfig(label, description, exclusiveClasses, overlappingClasses);
        AbstractClassifier<?, ?> classifier = classifierFactory.getClassifier(id, config, encodedFeatureSpecs);
        return classifier;
    }

    public AbstractClassifier<?,?> getById(String classifierId) throws ServiceException {
        if (!loaded) {
            loadKnownClassifiers();
        }
        if (!idMap.containsKey(classifierId)) {
            throw ServiceException.FAILURE(String.format("no classifier found with id=%s", classifierId), null);
        }
        return idMap.get(classifierId);
    }

    public AbstractClassifier<?,?> getByLabel(String classifierLabel) throws ServiceException {
        if (!loaded) {
            loadKnownClassifiers();
        }
        if (!labelMap.containsKey(classifierLabel)) {
            throw ServiceException.FAILURE(String.format("no classifier found with label=%s", classifierLabel), null);
        }
        return labelMap.get(classifierLabel);
    }

    @SuppressWarnings("unchecked")
    public <T extends Classifiable, R extends Classification> AbstractClassifier<T, R> delete(String classifierId) throws ServiceException {
        if (!loaded) {
            loadKnownClassifiers();
        }
        AbstractClassifier<T,R> classifier = (AbstractClassifier<T,R>) idMap.remove(classifierId);
        if (classifier != null) {
            deRegister(encode(classifier));
            labelMap.remove(classifier.getLabel());
        }
        return classifier;
    }

    protected abstract void deRegister(String encoded) throws ServiceException;
}
package com.zimbra.cs.ml.classifier;

import java.util.HashMap;
import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.ml.Classifiable;
import com.zimbra.cs.ml.feature.ComputedFeatures;
import com.zimbra.cs.ml.feature.FeatureSet;
import com.zimbra.cs.ml.query.AbstractClassificationQuery;
import com.zimbra.cs.ml.query.CreateClassifierQuery;
import com.zimbra.cs.ml.query.ListClassifiersQuery;
import com.zimbra.cs.ml.query.TrainClassifierQuery;
import com.zimbra.cs.ml.schema.ClassificationResult;
import com.zimbra.cs.ml.schema.ClassifierInfo;
import com.zimbra.cs.ml.schema.TrainingSpec;

/**
 * Classifier powered by Zimbra's ML server backend
 */
public abstract class Classifier <T extends Classifiable> extends AbstractClassifier<T, ClassificationResult> {

    private ClassifierInfo info = null;

    protected Classifier(String id, ClassifierConfig metadata, FeatureSet<T> featureSet, ClassifierInfo info) {
        super(id, metadata, featureSet);
        setClassifierInfo(info);
    }

    protected Classifier(String id, ClassifierConfig metadata, FeatureSet<T> featureSet) {
        this(id, metadata, featureSet, null);
    }

    public void setClassifierInfo(ClassifierInfo info) {
        this.info = info;
    }

    public ClassifierInfo getInfo() {
        return info;
    }


    public ClassifierInfo train(TrainingSpec trainingSpec) throws ServiceException {
        trainingSpec.setClassifierId(getId());
        TrainClassifierQuery query = new TrainClassifierQuery(trainingSpec);
        ClassifierInfo info = query.execute();
        this.info = info;
        return info;
    }

    protected abstract AbstractClassificationQuery<T> buildQuery(ComputedFeatures<T> features) throws ServiceException;


    @Override
    protected ClassificationResult classifyFeatures(ComputedFeatures<T> features) throws ServiceException {
        AbstractClassificationQuery<T> query = buildQuery(features);
        return query.execute();
    }

    public abstract static class Factory<T extends Classifiable> extends AbstractClassifier.Factory<T, ClassificationResult> {

        private Map<String, ClassifierInfo> infoMap = null;

        private Map<String, ClassifierInfo> loadInfoMap() throws ServiceException {
          Map<String, ClassifierInfo> infoMap = new HashMap<>();
          ZimbraLog.ml.debug("found %d classifiers registered with ML backend", infoMap.size());
          ListClassifiersQuery query = new ListClassifiersQuery();
          for (ClassifierInfo info: query.execute()) {
              infoMap.put(info.getClassifierId(), info);
          }
          return infoMap;
        }

        protected abstract AbstractClassifier<T, ClassificationResult> getClassifier(String id, ClassifierConfig config, FeatureSet<T> featureSet,
                ClassifierInfo classifierInfo) throws ServiceException;

        @Override
        public AbstractClassifier<T, ClassificationResult> getClassifier(String id, ClassifierConfig config, FeatureSet<T> featureSet) throws ServiceException {
            if (infoMap == null) {
                infoMap = loadInfoMap();
            }
            ClassifierInfo info = infoMap.get(id);
            if (info == null) {
                throw ServiceException.FAILURE(String.format("unable to construct classifier '%s' (id=%s), classifier not registered on ML server", config.getLabel(), id), null);
            }
            return getClassifier(id, config, featureSet, info);
        }
    }
}

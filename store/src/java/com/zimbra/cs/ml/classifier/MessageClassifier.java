package com.zimbra.cs.ml.classifier;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.mailbox.Message;
import com.zimbra.cs.ml.feature.ComputedFeatures;
import com.zimbra.cs.ml.feature.FeatureSet;
import com.zimbra.cs.ml.query.CreateClassifierQuery;
import com.zimbra.cs.ml.query.MessageClassificationQuery;
import com.zimbra.cs.ml.schema.ClassificationResult;
import com.zimbra.cs.ml.schema.ClassifierInfo;
import com.zimbra.cs.ml.schema.MessageClassificationInput;

public class MessageClassifier extends Classifier<Message> {

    public static final String TYPE = "zimbra-message";

    private MessageClassifier(String id, ClassifierConfig metadata, FeatureSet<Message> featureSet, ClassifierInfo info) {
        super(id, metadata, featureSet, info);
    }

    private MessageClassifier(String id, ClassifierConfig metadata, FeatureSet<Message> featureSet) {
        super(id, metadata, featureSet);
    }

    @Override
    protected MessageClassificationQuery buildQuery(ComputedFeatures<Message> features) throws ServiceException {
        Message msg = features.getItem();
        return new MessageClassificationQuery(getId(), msg.getRecipients(), new MessageClassificationInput(features));
    }

    @Override
    public String getFeatureSetType() {
        return "message";
    }

    @Override
    public String getClassifierType() {
        return TYPE;
    }

    /**
     * create a new classifier and register it with the ML backend
     */
    public static MessageClassifier init(ClassifierData<Message> data) throws ServiceException {
        CreateClassifierQuery query = new CreateClassifierQuery(data.getSpec());
        ClassifierInfo info = query.execute();
        return new MessageClassifier(info.getClassifierId(), data.getMetadata(), data.getFeatureSet(), info);
    }

    public static class Factory extends Classifier.Factory<Message> {

        @Override
        protected AbstractClassifier<Message, ClassificationResult> getClassifier(String id, ClassifierConfig config, FeatureSet<Message> featureSet,
                ClassifierInfo classifierInfo) throws ServiceException {
            return new MessageClassifier(id, config, featureSet, classifierInfo);
        }
    }
}

package com.zimbra.cs.ml;

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.ml.callback.ClassificationCallback;
import com.zimbra.cs.ml.callback.ExclusiveClassCallback;
import com.zimbra.cs.ml.callback.OverlappingClassCallback;
import com.zimbra.cs.ml.classifier.AbstractClassifier;
import com.zimbra.cs.ml.classifier.ClassifierManager;

/**
 * This class represents a known task to be performed with the classification system.
 */
public abstract class ClassificationTask<C extends Classifiable, R extends Classification> {

    private String taskName;
    private ClassificationType taskType;
    protected ExclusiveClassCallback<C> exclusiveCallback;

    public static enum ClassificationType {
        BINARY, MULTI_LABEL;
    }

    public ClassificationTask(String taskName, ClassificationType taskType) {
        this.taskName = taskName;
        this.taskType = taskType;
    }

    public String getTaskName() {
        return taskName;
    }

    public ClassificationType getTaskType() {
        return taskType;
    }

    public ClassificationTask<C, R> withExclusiveClassCallback(ExclusiveClassCallback<C> callback) {
        this.exclusiveCallback = callback;
        return this;
    }

    public boolean supportsClassifier(AbstractClassifier<C, R> classifier) {
        if (exclusiveCallback == null) {
            return false;
        }
        Set<String> classifierExclusiveClasses = Sets.newHashSet(classifier.getExclusiveClasses());
        return classifierExclusiveClasses.containsAll(exclusiveCallback.getExclusiveClasses());
    }

    protected abstract void checkCanRegister() throws ServiceException;

    public void register() {
        try {
            checkCanRegister();
            ZimbraLog.ml.info("registering classification task %s", taskName);
            ClassifierManager.getInstance().registerClassificationTask(this);
        } catch (ServiceException e) {
            ZimbraLog.ml.error("unable to register classification task %s", taskName, e);
        }
    }

    /**
     * Given a classifier, return the appropriate callback
     */
    public ClassificationCallback<C> resolveCallback(AbstractClassifier<C, R> classifier) throws ServiceException {
        String[] exclusiveClasses = classifier.getExclusiveClasses();
        if (exclusiveClasses == null) {
            String errMsg = String.format("cannot determine classification callback for task '%s' with classifier '%s'", taskName, classifier.getLabel());
            throw ServiceException.FAILURE(errMsg, null);
        } else {
            return exclusiveCallback;
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("task", taskName)
                .add("type", taskType.name()).toString();
    }

    /**
     * Definition of a multi-label classification task where the output is zero or one of
     * a set of possible labels.
     * Multi-label classification tasks must be handled with exclusive class callbacks.
     */
    public static class MultilabelClassificationTask<C extends Classifiable, R extends Classification> extends ClassificationTask<C, R> {

        public MultilabelClassificationTask(String taskName) {
            super(taskName, ClassificationType.MULTI_LABEL);
        }

        @Override
        protected void checkCanRegister() throws ServiceException {
            if (exclusiveCallback == null) {
                throw ServiceException.FAILURE("ExclusiveClassCallback must be specified prior to registering MultilabelClassificationTask", null);
            }
        }
    }

    /**
     * Definition of a binary classification task.
     * Binary classification tasks can be handled with either overlapping or exclusive class callbacks.
     */
    public static class BinaryClassificationTask<C extends Classifiable, R extends Classification> extends ClassificationTask<C, R> {

        private OverlappingClassCallback<C> overlappingCallback;

        public BinaryClassificationTask(String taskName) {
            super(taskName, ClassificationType.BINARY);
        }

        @Override
        public ClassificationCallback<C> resolveCallback(AbstractClassifier<C, R> classifier) throws ServiceException {
            String[] overlappingClasses = classifier.getOverlappingClasses();
            if (overlappingClasses != null && overlappingCallback != null) {
                for (String oc: overlappingClasses) {
                    if (oc.equalsIgnoreCase(overlappingCallback.getOverlappingClass())) {
                        return overlappingCallback;
                    }
                }
            }
            return super.resolveCallback(classifier);
        }

        public BinaryClassificationTask<C, R> withOverlappingClassCallback(OverlappingClassCallback<C> callback) {
            this.overlappingCallback = callback;
            return this;
        }

        @Override
        public boolean supportsClassifier(AbstractClassifier<C, R> classifier) {
            //the classifier needs to match EITHER the matching or exclusive callback class labels
            boolean matchesOverlappingCallback = false;
            boolean matchesExclusiveCallback = false;
            if (overlappingCallback != null) {
                String overlappingClassLabel = overlappingCallback.getOverlappingClass();
                Set<String> classifierOverlappingLabels = Sets.newHashSet(classifier.getOverlappingClasses());
                matchesOverlappingCallback = classifierOverlappingLabels.contains(overlappingClassLabel);
            }
            matchesExclusiveCallback = super.supportsClassifier(classifier);
            return matchesOverlappingCallback || matchesExclusiveCallback;
        }

        @Override
        protected void checkCanRegister() throws ServiceException {
            if (exclusiveCallback == null && overlappingCallback == null) {
                throw ServiceException.FAILURE("ExclusiveClassCallback or OverlappingClassCallback must be specified prior to registering BinaryClassificationTask", null);
            }
        }
    }
}

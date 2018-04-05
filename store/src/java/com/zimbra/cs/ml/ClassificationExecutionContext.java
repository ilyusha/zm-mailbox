package com.zimbra.cs.ml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.ml.classifier.AbstractClassifier;

/**
 * This class is a bridge between the mailbox and the classification system.
 * It provides a mapping of {@link Classifier} instances to corresponding {@link ClassificationHandler} instances
 * that lets us execute the known {@link ClassificationTask} definitions.
 * This abstracts away the details of which classifiers are invoked, as there may not be a one-to-one
 * correspondence between tasks and classifiers.
 */
public class ClassificationExecutionContext<C extends Classifiable, R extends Classification> {
    private Map<String, ClassifierUsageInfo<C, R>> classifierMap;

    public ClassificationExecutionContext() {
        classifierMap = new HashMap<>();
    }

    /**
     * Link a {@link ClassificationTask} to a {@link Classifier}
     */
    public void addClassifierForTask(ClassificationTask<C, R> task, AbstractClassifier<C, R> classifier) {
        ClassifierUsageInfo<C, R> usageInfo = classifierMap.get(classifier.getId());
        if (usageInfo == null) {
            usageInfo = new ClassifierUsageInfo<>(classifier);
            classifierMap.put(classifier.getId(), usageInfo);
        }
        usageInfo.addTask(task);
    }

    /**
     * Invoke each classifier and handle the classification output accordingly.
     */
    public void execute(C item) throws ServiceException {
        for (ClassifierUsageInfo<C, R> info: classifierMap.values()) {
            ClassificationHandler<C, R> handler = info.getHandler();
            handler.handle(item, info.getClassifier().classifyItem(item));
        }
    }

    /**
     * Get the {@link ClassificationHandler} for the given classifier
     */
    public ClassificationHandler<C, R> getHandler(AbstractClassifier<C, R> classifier) {
        ClassifierUsageInfo<C, R> info = classifierMap.get(classifier.getId());
        return info == null ? null : info.getHandler();
    }

    /**
     * Return a list of all {@link ClassifierUsageInfo} for this execution context
     */
    public List<ClassifierUsageInfo<C, R>> getInfo() {
        return new ArrayList<ClassifierUsageInfo<C, R>>(classifierMap.values());
    }

    /**
     * Return the {@link ClassifierUsageInfo} for the given classifier for this execution context
     */
    public ClassifierUsageInfo<C, R> getClassifierUsage(AbstractClassifier<C, R> classifier) {
        return classifierMap.containsKey(classifier.getId()) ? classifierMap.get(classifier.getId()) : new ClassifierUsageInfo<C, R>(classifier);
    }

    /**
     * Returns false if this execution context does not result in any tasks being processed
     */
    public boolean hasResolvedTasks() {
        return !classifierMap.isEmpty();
    }

    public static class ClassifierUsageInfo<C extends Classifiable, R extends Classification> {
        private AbstractClassifier<C, R> classifier;
        private ClassificationHandler<C, R> handler;
        private List<ClassificationTask<C, R>> tasks;

        ClassifierUsageInfo(AbstractClassifier<C, R> classifier) {
            this.classifier = classifier;
            this.handler = new ClassificationHandler<>();
            this.tasks = new ArrayList<>();
        }

        public AbstractClassifier<C, R> getClassifier() {
            return classifier;
        }

        public ClassificationHandler<C, R> getHandler() {
            return handler;
        }

        public void addTask(ClassificationTask<C, R> task) {
            tasks.add(task);
        }

        public List<ClassificationTask<C, R>> getTasks() {
            return tasks;
        }
    }
}
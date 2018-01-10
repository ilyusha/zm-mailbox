package com.zimbra.cs.ml.classifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.account.Server;
import com.zimbra.cs.mailbox.Message;
import com.zimbra.cs.ml.ClassificationTask.BinaryClassificationTask;
import com.zimbra.cs.ml.ClassificationTask.MultilabelClassificationTask;
import com.zimbra.cs.ml.ClassificationTaskConfigProvider;
import com.zimbra.cs.ml.callback.BinaryExclusiveSmartFolderCallback;
import com.zimbra.cs.ml.callback.MultilabelExclusiveSmartFolderCallback;
import com.zimbra.cs.ml.callback.OverlappingSmartFolderCallback;

/**
 * This is a repository of known classification tasks that get registered at startup.
 */
public final class SmartFolderClassificationTasks {

    private static final String EXCLUSIVE_SMART_FOLDERS_TASK_NAME = "exclusive-smart-folders";
    private static final String OVERLAPPING_SMART_FOLDERS_TASK_FMT = "folder-%s";

    static Set<String> OVERLAPPING_SMART_FOLDER_TASKS = new HashSet<>();

    private static Map<String, String> buildSmartFolderMap(String[] configStrings) throws ServiceException {
        Map<String, String> map = new HashMap<>();
        if (configStrings == null) {
            return map;
        }
        for (String config: configStrings) {
            String key; String value;
            if (config.indexOf(":") < 0) {
                key = value = config;
            } else {
                String[] toks = config.split(":", 2);
                key = toks[0];
                value = toks[1];
            }
            if (map.containsKey(key)) {
                //can't have duplicate class label mappings
                throw ServiceException.FAILURE("duplicate exclusive smart folder class label " + key, null);
            }
            map.put(key, value);
        }
        return map;
    }

    public static String getOverlappingSmartFolderTaskName(String smartFolderName) {
        return String.format(OVERLAPPING_SMART_FOLDERS_TASK_FMT, smartFolderName);
    }

    /**
     * Delete exclusive SmartFolder classification task and its classifier assignment
     */
    public static void clearExclusiveTask(ClassificationTaskConfigProvider taskConfig) throws ServiceException {
        ClassifierManager manager = ClassifierManager.getInstance();
        manager.deleteClassificationTask(EXCLUSIVE_SMART_FOLDERS_TASK_NAME, taskConfig);
    }

    public static void clearOverlappingTasks(ClassificationTaskConfigProvider taskConfig) throws ServiceException {
        ClassifierManager manager = ClassifierManager.getInstance();
        for (String taskName: OVERLAPPING_SMART_FOLDER_TASKS) {
            manager.deleteClassificationTask(taskName, taskConfig);
        }
    }

    public static void registerExclusiveSmartFolderTask() throws ServiceException {
        Server server = Provisioning.getInstance().getLocalServer();
        String[] smartFolders = server.getExclusiveSmartFolders();
        Map<String, String> exclusiveSmartFolderMap = buildSmartFolderMap(smartFolders);
        //Exclusive classes are mapped to a single classification task, since it must be handled by one classifier
        new MultilabelClassificationTask<Message>(EXCLUSIVE_SMART_FOLDERS_TASK_NAME)
        .withExclusiveClassCallback(new MultilabelExclusiveSmartFolderCallback(exclusiveSmartFolderMap))
        .register();
    }

    public static void registerOverlappingSmartFolderTask(String overlappingClassName, String smartFolderName) {
        String taskName = getOverlappingSmartFolderTaskName(smartFolderName.toLowerCase());
        OVERLAPPING_SMART_FOLDER_TASKS.add(taskName);
        new BinaryClassificationTask<Message>(taskName)
        .withOverlappingClassCallback(new OverlappingSmartFolderCallback(overlappingClassName, smartFolderName))
        .withExclusiveClassCallback(new BinaryExclusiveSmartFolderCallback(overlappingClassName, smartFolderName))
        .register();
    }

    public static void registerOverlappingSmartFolderTasks() throws ServiceException {
        Server server = Provisioning.getInstance().getLocalServer();
        Map<String, String> overlappingSmartFolderMap = buildSmartFolderMap(server.getOverlappingSmartFolders());
        //Each overlapping class is mapped to a separate classification task
        for (Map.Entry<String, String> entry: overlappingSmartFolderMap.entrySet()) {
            String overlappingClassName = entry.getKey();
            String smartFolderName = entry.getValue();
            registerOverlappingSmartFolderTask(overlappingClassName, smartFolderName);
        }
    }

    /**
     * Register classification tasks based on the values of zimbraOverlappingSmartFolders and zimbraExclusiveSmartFolders
     * LDAP attributes
     */
    public static void registerTasks() {
        try {
            registerExclusiveSmartFolderTask();
            registerOverlappingSmartFolderTasks();
        } catch (ServiceException e) {
            ZimbraLog.ml.error("error registering SmartFolder classification tasks", e);
        }
    }
}

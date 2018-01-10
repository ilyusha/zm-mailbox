package com.zimbra.cs.account.callback;

import java.util.List;
import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.Pair;
import com.zimbra.cs.account.AttributeCallback;
import com.zimbra.cs.account.Entry;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.ml.ClassificationTaskConfigProvider;
import com.zimbra.cs.ml.LdapClassificationTaskConfigProvider;
import com.zimbra.cs.ml.classifier.ClassifierManager;
import com.zimbra.cs.ml.classifier.SmartFolderClassificationTasks;

public class ResolveClassifiers extends AttributeCallback {

    @Override
    public void preModify(CallbackContext context, String attrName,
            Object attrValue, Map attrsToModify, Entry entry) throws ServiceException {
        ClassifierManager manager = ClassifierManager.getInstance();
        if (attrName.equals(Provisioning.A_zimbraExclusiveSmartFolders)) {
            /*Since exclusive smart folders are treated as a single classification task,
             *changing the folders in any way likely requires a new classifier to be used.
             Hence, we clear the classification task. */
            SmartFolderClassificationTasks.clearExclusiveTask(new LdapClassificationTaskConfigProvider());
            SmartFolderClassificationTasks.registerExclusiveSmartFolderTask();
        } else if (attrName.equals(Provisioning.A_zimbraOverlappingSmartFolders)) {
            MultiValueMod mod = multiValueMod(attrsToModify, Provisioning.A_zimbraOverlappingSmartFolders);
            List<String> values = mod.values();
            if (mod.deleting()) {
                SmartFolderClassificationTasks.clearOverlappingTasks(new LdapClassificationTaskConfigProvider());
            } else if (mod.adding()) {
                //if we're adding a new overlapping smart folder, we should add a new classification task
                //and leave the existing ones untouched
                for (String val: values) {
                    Pair<String, String> parsed = parseVal(val);
                    SmartFolderClassificationTasks.registerOverlappingSmartFolderTask(parsed.getFirst(), parsed.getSecond());
                }
            } else if (mod.removing()) {
                //if we're deleting an overlapping smart folder, we should delete its task and
                //classifier assignment
                for (String toRemove: values) {
                    Pair<String, String> parsed = parseVal(toRemove);
                    String taskName = SmartFolderClassificationTasks.getOverlappingSmartFolderTaskName(parsed.getSecond());
                    ClassificationTaskConfigProvider config = new LdapClassificationTaskConfigProvider();
                    manager.deleteClassificationTask(taskName, config);
                }

            } else if (mod.replacing()) {
                //if replacing the value, we need to clear the existing tasks and register the new ones
                SmartFolderClassificationTasks.clearOverlappingTasks(new LdapClassificationTaskConfigProvider());
                for (String val: values) {
                    Pair<String, String> parsed = parseVal(val);
                    SmartFolderClassificationTasks.registerOverlappingSmartFolderTask(parsed.getFirst(), parsed.getSecond());
                }
            }
        }
        manager.clearExecutionContextCache();
    }

    private static Pair<String, String> parseVal(String val) {
        if (val.indexOf(":") < 0) {
            return new Pair<>(val, val);
        } else {
            String[] toks = val.split(":", 2);
            return new Pair<>(toks[0], toks[1]);
        }
    }

    @Override
    public void postModify(CallbackContext context, String attrName, Entry entry) {}
}

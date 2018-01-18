package com.zimbra.cs.ml;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.AdminConstants;
import com.zimbra.common.util.CliUtil;
import com.zimbra.common.util.Log.Level;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.account.Server;
import com.zimbra.cs.account.soap.SoapProvisioning;
import com.zimbra.cs.httpclient.URLUtil;
import com.zimbra.cs.ml.ClassificationExecutionContext.ClassifierUsageInfo;
import com.zimbra.cs.ml.ClassificationTaskConfigProvider.TaskConfig;
import com.zimbra.cs.ml.callback.ExclusiveClassCallback;
import com.zimbra.cs.ml.callback.OverlappingClassCallback;
import com.zimbra.cs.ml.classifier.Classifier;
import com.zimbra.cs.ml.classifier.ClassifierData;
import com.zimbra.cs.ml.classifier.ClassifierManager;
import com.zimbra.cs.ml.classifier.ClassifierXmlLoader;
import com.zimbra.cs.ml.classifier.SmartFolderClassificationTasks;
import com.zimbra.cs.ml.feature.FeatureParams;
import com.zimbra.cs.ml.feature.FeatureSet;
import com.zimbra.cs.ml.feature.FeatureSpec;
import com.zimbra.cs.ml.feature.FeatureSpec.KnownFeature;
import com.zimbra.cs.ml.schema.ClassifierInfo;
import com.zimbra.cs.ml.schema.TrainingData;
import com.zimbra.cs.ml.schema.TrainingSetInfo;
import com.zimbra.cs.ml.schema.TrainingSpec;
import com.zimbra.soap.admin.type.CacheEntryType;

public class ClassifierUtil {

    private static final String O_EPOCHS = "e";
    private static final String O_TRAINING_RATE = "r";
    private static final String O_TEST_HOLDOUT = "t";
    private static final String O_PERSIST = "p";
    private static final String O_THRESHOLD = "threshold";
    private static final String O_DEBUG = "d";

    private static Options OPTIONS = new Options();

    private static PrintStream out = System.out;
    private static ClassifierManager manager;
    private static CommandLine opts;

    private static enum Command {
        LIST_CLASSIFIERS("list", "l", null, "list known classifiers"),
        SHOW_CLASSIFIER("show", "s", "{classifier}", "show details on the classifier with the given name or ID"),
        TRAIN("train", "tr","{classifier}", "train the given classifier (incomplete)"),
        DELETE("delete", "d", "{classifier}", "delete the classifier with the given name or ID"),
        TASKS("tasks", "t", null, "list known classification tasks"),
        ASSIGN("assign", "a", "{task} {classifier}", "assign a classification task to classifier with the given name or ID"),
        UNASSIGN("unassign", "u", "{task}", "clear the classifier assignment for the given task"),
        RESOLVE("resolve", null, null, "resolve task-classifier assignments"),
        REGISTER("register", "r", "{classifier}", "register a new classifier using an XML configuration");

        private String longName;
        private String shortName;
        private String argsDesc;
        private String description;

        private Command(String longName, String shortName, String argsDesc, String description) {
            this.longName = longName;
            this.shortName = shortName;
            this.argsDesc = argsDesc;
            this.description = description;
        }

        static Command of(String cmd) throws ServiceException {
            for (Command c: Command.values()) {
                if (c.longName.equalsIgnoreCase(cmd) || (c.shortName != null && c.shortName.equalsIgnoreCase(cmd))) {
                    return c;
                }
            }
            usage();
            return null;
        }

        String getUsage() {
            String cmd = shortName == null ? longName : String.format("%s(%s)", longName, shortName);
            if (argsDesc != null) {
                cmd = String.format("%s %s", cmd, argsDesc);
            }
            return String.format("  %-35s %s", cmd, description);
        }
    }

    static {
        OPTIONS.addOption(O_EPOCHS, "epochs", true, "Number of epochs to train the classifier");
        OPTIONS.addOption(O_TRAINING_RATE, "training-rate", true, "Training rate for classifier training; must be between 0 and 1");
        OPTIONS.addOption(O_TEST_HOLDOUT, "test-holdout", true, "Percent holdout for classifier training; must be between 0 and 1");
        OPTIONS.addOption(O_PERSIST, "persist", false, "Whether to persist the training set on the machine learning server");
        OPTIONS.addOption(O_THRESHOLD, true, "Percent threshold to be used when assigning a classifier to a task using overlapping classes");
        OPTIONS.addOption(O_DEBUG, "debug", false, "Enable debug logging");
    }

    public static void main(String[] cmdArgs) throws Exception {
        CliUtil.toolSetup();
        CommandLineParser parser = new GnuParser();
        opts = parser.parse(OPTIONS, cmdArgs);
        String[] args = opts.getArgs();
        if (opts.hasOption(O_DEBUG)) {
            ZimbraLog.ml.setLevel(Level.debug);
        } else {
            ZimbraLog.ml.setLevel(Level.warn);
        }
        ZimbraLog.ephemeral.setLevel(Level.debug);

        manager = ClassifierManager.getInstance();
        SmartFolderClassificationTasks.registerTasks();

        if (args.length == 0) {
            usage();
            return;
        }
        Command command = Command.of(args[0]);
        switch (command) {
        case LIST_CLASSIFIERS:
            doListClassifiers();
            break;
        case SHOW_CLASSIFIER:
            doShowClassifier(args);
            break;
        case DELETE:
            doDeleteClassifier(args);
            break;
        case TRAIN:
            doTrainClassifier(args);
            break;
        case TASKS:
            doListTasks();
            break;
        case RESOLVE:
            doResolve(args);
            break;
        case ASSIGN:
            doAssign(args);
            break;
        case UNASSIGN:
            doUnassign(args);
            break;
        case REGISTER:
            doRegister(args);
        default:
            break;
        }
    }

    private static void dumpClassInfo(ClassifierInfo info) {
        if (info.getExclusiveClasses().length > 0) {
            out.println(String.format("exclusive classes:   %s", Joiner.on(", ").join(info.getExclusiveClasses())));
        }
        if (info.getOverlappingClasses().length > 0) {
            out.println(String.format("overlapping classes: %s", Joiner.on(", ").join(info.getOverlappingClasses())));
        }
    }

    private static void dumpClassifierInfo(ClassifierInfo info) {
        if (info == null) { return; }
        dumpClassInfo(info);
        out.println(String.format("num body words:      %d", info.getNumBodyWords()));
        out.println(String.format("num subject words:   %d", info.getNumSubjectWords()));
        out.println(String.format("num features:        %d", info.getNumFeatures()));
        out.println(String.format("epoch:               %d", info.getEpoch()));
        if (info.getVocabPath() != null) {
            out.println(String.format("vocab path:          %s", info.getVocabPath()));
        }
        dumpTrainingSet(info.getTrainingSet());
    }

    private static void dumpTrainingSet(TrainingSetInfo trainingSetInfo) {
        if (trainingSetInfo == null) { return; }
        out.println("Training Set Info:");
        out.println(String.format("Date:      %s", trainingSetInfo.getDate()));
        out.println(String.format("Num train: %s", trainingSetInfo.getNumTrain()));
        out.println(String.format("Num test:  %s", trainingSetInfo.getNumTest()));
    }

    private static void dumpFeatures(FeatureSet<?> featureSet) {
        if (featureSet == null) { return; }
        out.println("Features:");
        for (FeatureSpec<?> spec: featureSet.getAllFeatureSpecs()) {
            KnownFeature feature = spec.getFeature();
            FeatureParams params = spec.getParams();
            if (params.getParams().isEmpty()) {
                out.println(String.format(" - %s", feature));
            } else {
                out.println(String.format(" - %s (%s)", feature, params));
            }
        }
    }

    private static void dumpClassifierDescription(Classifier<?> classifier, int headerWidth) {
        String desc = classifier.getDescription();
        if (!Strings.isNullOrEmpty(desc)) {
            out.println(WordUtils.wrap(desc, headerWidth));
            out.println(StringUtils.repeat("-", headerWidth));
        }
    }

    private static int dumpClassifierHeader(Classifier<?> classifier) {
        String header = String.format("[Classifier name=%s, id=%s]", classifier.getLabel(), classifier.getId());
        out.println(header);
        return header.length();
    }

    private static void dumpClassifier(Classifier<?> classifier) {
        if (classifier == null) { return; }
        int width = dumpClassifierHeader(classifier);
        out.println(StringUtils.repeat("-", width));
        dumpClassifierDescription(classifier, width);
        dumpClassifierInfo(classifier.getInfo());
        dumpFeatures(classifier.getFeatureSet());
    }

    private static void dumpClassifierSummary(Classifier<?> classifier) {
        if (classifier == null) { return; }
        dumpClassifierHeader(classifier);
    }

    private static int stringArrLength(String[] arr) {
        return Joiner.on(", ").join(arr).length();
    }

    private static void doListClassifiers() throws ServiceException {

        Collection<Classifier<?>> classifiers = manager.getAllClassifiers().values();
        if (classifiers.isEmpty()) {
            out.println("no classifiers found");
            return;
        }
        int nameColWidth = classifiers.stream().map(c -> c.getLabel().length()).max(Integer::compare).get() + 5;
        int idColWidth = classifiers.stream().map(c -> c.getId().length()).max(Integer::compare).get() + 5;
        int featureColWidth ="#Features".length() + 5;
        int exClassesColWidth = classifiers.stream().map(c -> stringArrLength(c.getInfo().getExclusiveClasses())).max(Integer::compare).get() + 5;
        int ovClassesColWidth = classifiers.stream().map(c -> stringArrLength(c.getInfo().getOverlappingClasses())).max(Integer::compare).get() + 5;
        String idColHeader = StringUtils.rightPad("ID", idColWidth);
        String nameColHeader = StringUtils.rightPad("Name", nameColWidth);
        String featuresColHeader = StringUtils.rightPad("#Features", featureColWidth);
        String exClassesColHeader = StringUtils.rightPad("Exclusive Classes", exClassesColWidth);
        String ovClassesColHeader = StringUtils.rightPad("Overlapping Classes", ovClassesColWidth);
        String fmt = "%s%s%s%s%s";
        String header = String.format(fmt, idColHeader, nameColHeader, featuresColHeader, exClassesColHeader, ovClassesColHeader);
        out.println("\n" + header + "\n" + StringUtils.repeat("-", header.length()));
        for (Classifier<?> classifier: classifiers) {
            String id = StringUtils.rightPad(classifier.getId(), idColWidth);
            String name = StringUtils.rightPad(classifier.getLabel(), nameColWidth);
            String numFeatures = StringUtils.rightPad(String.valueOf(classifier.getFeatureSet().getNumFeatures()), featureColWidth);
            String exClasses = StringUtils.rightPad(Joiner.on(", ").join(classifier.getInfo().getExclusiveClasses()), exClassesColWidth);
            String ovClasses = StringUtils.rightPad(Joiner.on(", ").join(classifier.getInfo().getOverlappingClasses()), ovClassesColWidth);
            out.println(String.format(fmt, id, name, numFeatures, exClasses, ovClasses));
        }
    }

    private static void doShowClassifier(String[] args) throws ServiceException {
        dumpClassifier(getByLabelOrId(manager, args[1]));
    }

    private static void doDeleteClassifier(String[] args) throws ServiceException {
        Classifier<?> classifier = getByLabelOrId(manager, args[1]);
        out.print("Deleted: ");
        dumpClassifierSummary(manager.deleteClassifier(classifier.getId()));
        flushClassifierCache();
    }

    private static int getNumEpochs() throws ServiceException {
        try {
            if (!opts.hasOption(O_EPOCHS)) {
                throw ServiceException.INVALID_REQUEST("must specify number of epochs", null);
            }
            int numEpochs = Integer.valueOf(opts.getOptionValue(O_EPOCHS));
            if (numEpochs <= 0) {
                throw ServiceException.INVALID_REQUEST("number of epochs must be positive", null);
            } else {
                return numEpochs;
            }
        } catch (NumberFormatException e) {
            throw ServiceException.INVALID_REQUEST("number of epochs must be a positive integer", null);
        }
    }

    private static float getTrainRate() throws ServiceException {
        try {
            if (!opts.hasOption(O_TRAINING_RATE)) {
                throw ServiceException.INVALID_REQUEST("must specify a training rate", null);
            }
            float trainRate = Float.valueOf(opts.getOptionValue(O_TRAINING_RATE));
            if (trainRate <= 0 || trainRate > 1) {
                throw ServiceException.INVALID_REQUEST("training rate must be between 0 and 1", null);
            } else {
                return trainRate;
            }
        } catch (NumberFormatException e) {
            throw ServiceException.INVALID_REQUEST("training rate must be a number between 0 and 1", null);
        }
    }

    private static float getHoldout() throws ServiceException {
        try {
            if (!opts.hasOption(O_TEST_HOLDOUT)) {
                throw ServiceException.INVALID_REQUEST("must specify a holdout value", null);
            }
            float holdout = Float.valueOf(opts.getOptionValue(O_TEST_HOLDOUT));
            if (holdout <= 0 || holdout > 1) {
                throw ServiceException.INVALID_REQUEST("holdout must be between 0 and 1", null);
            } else {
                return holdout;
            }
        } catch (NumberFormatException e) {
            throw ServiceException.INVALID_REQUEST("holdout must be a number between 0 and 1", null);
        }
    }

    private static float getThreshold() throws ServiceException {
        try {
            float threshold = Float.valueOf(opts.getOptionValue(O_THRESHOLD));
            if (threshold <= 0 || threshold > 1) {
                throw ServiceException.INVALID_REQUEST("threshold value must be between 0 and 1", null);
            } else {
                return threshold;
            }
        } catch (NumberFormatException e) {
            throw ServiceException.INVALID_REQUEST("threshold must be a number between 0 and 1", null);
        }
    }

    private static void doTrainClassifier(String[] args) throws ServiceException {
        Classifier<?> classifier = getByLabelOrId(manager, args[1]);
        TrainingData data = new TrainingData();
        TrainingSpec spec = new TrainingSpec(data);
        int numEpochs = getNumEpochs();
        float trainingRate = getTrainRate();
        float holdout = getHoldout();
        boolean persist = opts.hasOption(O_PERSIST);
        out.println(String.format("Training classifier '%s' (epochs=%d, trainingRate=%.1f, holdout=%.1f, persist=%s", classifier.getLabel(), numEpochs, trainingRate, holdout, persist));
        spec.setEpochs(numEpochs);
        spec.setLearningRate(trainingRate);
        spec.setPercentHoldout(holdout);
        spec.setPersist(persist);
        dumpTrainingSet(classifier.train(spec).getTrainingSet());
    }

    private static void doListTasks() throws ServiceException {
        ClassificationTaskConfigProvider config = new LdapClassificationTaskConfigProvider();
        Map<String, TaskConfig> assignments = config.getConfigMap();
        Collection<ClassificationTask<?>> tasks = manager.getAllTasks();
        if (tasks.isEmpty()) {
            out.println("no classification tasks found");
            return;
        }
        int nameColWidth = tasks.stream().map(t -> t.getTaskName().length()).max(Integer::compare).get() + 5;
        int typeColWidth = tasks.stream().map(t -> t.getTaskType().name().length()).max(Integer::compare).get() + 5;
        String nameColHeader = StringUtils.rightPad("Task Name", nameColWidth);
        String typeColHeader = StringUtils.rightPad("Task Type", typeColWidth);
        String clsfColHeader = "Classifier";
        String fmt = "%s%s%s";
        String header = String.format(fmt, nameColHeader, typeColHeader, clsfColHeader);
        out.println("\n" + header + "\n" + StringUtils.repeat("-", header.length()));
        for (ClassificationTask<?> task: tasks) {
            String taskName = task.getTaskName();
            String name = StringUtils.rightPad(taskName, nameColWidth);
            String type = StringUtils.rightPad(task.getTaskType().name(), typeColWidth);
            String classifier;
            TaskConfig assignment = assignments.get(taskName);
            if (assignment == null) {
                classifier = "<UNASSIGNED>";
            } else if (assignment.getThreshold() != null) {
                classifier = String.format("%s (t=%.2f)", assignments.get(taskName), assignment.getThreshold());
            } else {
                classifier = assignment.getClassifierLabel();
            }
            out.println(String.format(fmt, name, type, classifier));
        }
    }

    private static <T extends Classifiable> void dumpExecutionContext(ClassificationExecutionContext<T> context) {
        for (ClassifierUsageInfo<T> info : context.getInfo()) {
            Classifier<T> classifier = info.getClassifier();
            ClassificationHandler<T> handler = info.getHandler();
            out.println(String.format("\nClassifier \"%s\" (id=%s)", classifier.getLabel(), classifier.getId()));
            out.println(String.format("- Used for tasks: [%s]", Joiner.on(", ").join(info.getTasks().stream().map(t -> t.getTaskName()).collect(Collectors.toList()))));
            if (handler.hasExclusiveCallback()) {
                ExclusiveClassCallback<T> exclusiveCallback = handler.getExclusiveClassCallback();
                out.println(String.format("- Exclusive class callback: %s", exclusiveCallback.getClass().getSimpleName()));
            }
            if (handler.getNumOverlappingClassCallbacks() > 0) {
                out.println("- Overlapping class callbacks:");
                for (Map.Entry<String, OverlappingClassCallback<T>> callbackEntry: handler.getOverlappingClassCallbacks().entrySet()) {
                    out.println(String.format("  - class '%s': %s", callbackEntry.getKey(), callbackEntry.getValue().getClass().getSimpleName()));
                }
            }
        }
    }

    private static void doResolve(String[] args) throws ServiceException {
        dumpExecutionContext(manager.resolveConfig(new LdapClassificationTaskConfigProvider()));
    }

    private static void doAssign(String[] args) throws ServiceException {
        String taskName = args[1];
        String classifierLabel = args[2];
        checkTaskExists(taskName);
        checkClassifierExists(classifierLabel);
        LdapClassificationTaskConfigProvider config = new LdapClassificationTaskConfigProvider();
        Classifier<?> classifier = getByLabelOrId(manager, classifierLabel);
        if (opts.hasOption(O_THRESHOLD)) {
            config.assignClassifier(taskName, classifier, getThreshold());
        } else {
            config.assignClassifier(taskName, classifier);
        }
        manager.resolveConfig(config);
        doListTasks();
        flushClassifierCache();
    }

    private static void doUnassign(String[] args) throws ServiceException {
        String taskName = args[1];
        checkTaskExists(taskName);
        LdapClassificationTaskConfigProvider config = new LdapClassificationTaskConfigProvider();
        config.clearAssignment(taskName);
        manager.resolveConfig(config);
        doListTasks();
        flushClassifierCache();
    }

    private static void doRegister(String[] args) throws ServiceException {
        String fileOrDir = args[1];
        File path = new File(fileOrDir);
        ClassifierXmlLoader loader = new ClassifierXmlLoader();
        List<ClassifierData<?>> specs = path.isDirectory() ? loader.loadDirectory(path) : loader.loadFile(path);
        for (ClassifierData<?> spec: specs) {
            String label = spec.getLabel();
            if (manager.labelExists(label)) {
                ZimbraLog.ml.warn("classifier with label \"%s\" already exists",label);
                continue;
            }
            dumpClassifier(manager.registerClassifier(spec));
        }
    }

    private static Classifier<?> getByLabelOrId(ClassifierManager manager, String labelOrId) throws ServiceException {
        try {
            return manager.getClassifierById(labelOrId);
        } catch (ServiceException e) {
            try {
                return manager.getClassifierByLabel(labelOrId);
            } catch (ServiceException e2) {}
        }
        out.println("no classifier found with ID or label " + labelOrId);
        System.exit(0);
        return null;
    }

    private static void checkClassifierExists(String label) throws ServiceException {
        if (!manager.labelExists(label)) {
            String known = Joiner.on(", ").join(manager.getAllClassifiers().values().stream().map(c -> c.getLabel()).collect(Collectors.toList()));
            throw ServiceException.FAILURE(String.format("'%s' is not a known classifier label, specify one of [%s]", label, known), null);
        }
    }

    private static void checkTaskExists(String taskName) throws ServiceException {
        ClassifierManager manager = ClassifierManager.getInstance();
        Collection<ClassificationTask<?>> allTasks = manager.getAllTasks();
        for (ClassificationTask<?> task: allTasks) {
            if (task.getTaskName().equalsIgnoreCase(taskName)) {
                return;
            }
        }
        String known = Joiner.on(", ").join(allTasks.stream().map(t -> t.getTaskName()).collect(Collectors.toList()));
        throw ServiceException.FAILURE(String.format("'%s' is not a known classification task, specify one of [%s]",  taskName, known), null);
    }

    private static void flushClassifierCache() throws ServiceException {
        SoapProvisioning soapProv = new SoapProvisioning();
        Server localServer = Provisioning.getInstance().getLocalServer();
        try {
            String adminUrl = URLUtil.getAdminURL(localServer, AdminConstants.ADMIN_SERVICE_URI, true);
            soapProv.soapSetURI(adminUrl);
        } catch (ServiceException e) {
            ZimbraLog.ml.warn("could not get admin URL for local server for clearning classifier resolution cache", e);
            return;
        }
        try {
            soapProv.soapZimbraAdminAuthenticate();
            soapProv.flushCache(CacheEntryType.classifier.name(), null, true, false);
            ZimbraLog.ml.info("sent FlushCache request to clear classifier resolution cache");

        } catch (ServiceException e) {
            ZimbraLog.ml.warn("cannot send FlushCache request to clear classifier resolution cache", e);
        }
    }

    @SuppressWarnings("PMD.DoNotCallSystemExit")
    private static void usage() {
        List<String> commands = new ArrayList<>();
        for (Command cmd: Command.values()) {
            commands.add(cmd.getUsage());
        }
        out.println("\ncommands:");
        out.println(String.format("%s\n", Joiner.on("\n").join(commands)));
        HelpFormatter format = new HelpFormatter();
        format.printHelp(new PrintWriter(System.err, true), 80,
            "zmclassifierutil command [options]", null, OPTIONS, 2, 2, null);
        System.exit(0);
    }
}

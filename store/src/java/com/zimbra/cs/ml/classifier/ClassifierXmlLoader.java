package com.zimbra.cs.ml.classifier;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.BooleanUtils;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.W3cDomUtil;
import com.zimbra.common.soap.XmlParseException;
import com.zimbra.cs.event.Event.EventType;
import com.zimbra.cs.event.analytics.contact.ContactAnalytics.ContactFrequencyEventType;
import com.zimbra.cs.event.analytics.contact.ContactAnalytics.ContactFrequencyTimeRange;
import com.zimbra.cs.mailbox.Message;
import com.zimbra.cs.ml.classifier.Classifier.ClassifiableType;
import com.zimbra.cs.ml.feature.FeatureParam;
import com.zimbra.cs.ml.feature.FeatureParam.ParamKey;
import com.zimbra.cs.ml.feature.FeatureSet;
import com.zimbra.cs.ml.feature.FeatureSpec;
import com.zimbra.cs.ml.feature.FeatureSpec.KnownFeature;
import com.zimbra.cs.ml.feature.NumRecipientsFeatureFactory.RecipientCountType;
import com.zimbra.cs.ml.schema.ClassifierSpec;

/**
 * Reads a classifier definition from an XML file
 */
public class ClassifierXmlLoader {

    private static final String E_CLASSIFIERS = "classifiers";
    private static final String E_CLASSIFIER = "classifier";
    private static final String E_ID = "id";
    private static final String E_NAME = "name";
    private static final String E_DESCRIPTION = "description";
    private static final String E_EXCLUSIVE_CLASSES = "exclusiveClasses";
    private static final String E_OVERLAPPING_CLASSES = "overlappingClasses";
    private static final String E_NUM_BODY_WORDS = "numBodyWords";
    private static final String E_NUM_SUBJ_WORDS = "numSubjectWords";
    private static final String E_VOCAB_PATH = "vocabPath";
    private static final String E_FEATURES = "features";
    private static final String E_FEATURE = "feature";
    private static final String E_TYPE = "type";

    public List<ClassifierData<?>> loadDirectory(File dir) throws ServiceException {
        List<ClassifierData<?>> classifiers = new ArrayList<>();
        if (!dir.exists()) {
            throw ServiceException.FAILURE("classifier directory does not exist: " + dir, null);
        }
        if (!dir.isDirectory()) {
            throw ServiceException.FAILURE("classifier directory is not a directory: " + dir, null);
        }

        File[] files = dir.listFiles();
        for (File file: files) {
            classifiers.addAll(loadFile(file));
        }
        return classifiers;
    }

    public List<ClassifierData<?>> loadFile(File file) throws ServiceException {
        List<ClassifierData<?>> classifiers = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file)) {
            Document doc = W3cDomUtil.parseXMLToDom4jDocUsingSecureProcessing(fis);
            Element root = doc.getRootElement();
            if (root.getName().equals(E_CLASSIFIERS)) {
                for (Iterator iter = root.elementIterator(); iter.hasNext();) {
                    classifiers.add(loadClassifier((Element) iter.next()));
                }
            }
            if (root.getName().equals(E_CLASSIFIER)) {
                classifiers.add(loadClassifier(doc.getRootElement()));
            }

        } catch (IOException | XmlParseException ex) {
            throw ServiceException.FAILURE("error loading attrs file: " + file, ex);
        }
        return classifiers;
    }

    private ClassifierData<?> loadClassifier(Element node) throws ServiceException  {
        Attribute attr = node.attribute(E_TYPE);
        if (attr == null) {
            throw ServiceException.FAILURE("classifier XML element must have an \"type\" attribute", null);
        }
        String type = attr.getValue();
        if (type.equals("message")) {
            return loadMessageClassifier(node);
        } else {
            throw ServiceException.FAILURE(String.format("unknown \"objectType\" value %s", type), null);
        }
    }

    private String getRequiredElement(Element node, String elementName) throws ServiceException {
        Element elt = node.element(elementName);
        if (elt == null) {
            throw ServiceException.FAILURE("missing required element " + elementName, null);
        }
        return elt.getText();
    }

    private String getOptionalElement(Element node, String elementName, String defaultStr) throws ServiceException {
        Element elt = node.element(elementName);
        if (elt == null) {
            return defaultStr;
        }
        return elt.getText();
    }

    private ClassifierData<Message> loadMessageClassifier(Element node) throws ServiceException {
        String name = getRequiredElement(node, E_NAME);
        String exclusiveClassesCsv = getOptionalElement(node, E_EXCLUSIVE_CLASSES, null);
        String overlappingClassesCsv = getOptionalElement(node, E_OVERLAPPING_CLASSES, null);
        if (exclusiveClassesCsv == null && overlappingClassesCsv == null) {
            throw ServiceException.FAILURE("must specify either exclusiveClasses, overlappingClasses, or both", null);
        }
        String[] exclusiveClasses = exclusiveClassesCsv == null ? null : exclusiveClassesCsv.split(",");
        String[] overlappingClasses = overlappingClassesCsv == null ? null : overlappingClassesCsv.split(",");
        int numSubjWords = Integer.valueOf(getRequiredElement(node, E_NUM_SUBJ_WORDS));
        int numBodyWords = Integer.valueOf(getRequiredElement(node, E_NUM_BODY_WORDS));
        String vocabPath = getOptionalElement(node,  E_VOCAB_PATH, null);
        String id = getOptionalElement(node,  E_ID, null);
        ClassifierSpec spec = new ClassifierSpec(numSubjWords, numBodyWords, exclusiveClasses, overlappingClasses);
        if (vocabPath != null) {
            spec.setVocabPath(vocabPath);
        }
        if (id != null) {
            spec.setClassifierId(id);
        }

        Element featuresElt = node.element(E_FEATURES);
        if (featuresElt == null) {
            throw ServiceException.FAILURE("must specify feature element", null);
        }
        FeatureSet<Message> fs = new FeatureSet<>();
        for (Iterator eltIter = featuresElt.elementIterator(); eltIter.hasNext();) {
            Element elt = (Element) eltIter.next();
            if (!elt.getName().equals(E_FEATURE)) {
                throw ServiceException.FAILURE("unsupported feature element " + elt.getName(), null);
            }
            List<FeatureParam<?>> params = new ArrayList<FeatureParam<?>>();
            KnownFeature featureType = null;
            for (Iterator attrIter = elt.attributeIterator(); attrIter.hasNext();) {
                Attribute attr = (Attribute) attrIter.next();
                String attrName = attr.getName();
                String value = attr.getValue();
                if (attrName.equals(E_TYPE)) {
                    featureType = KnownFeature.of(value);
                } else {
                    ParamKey paramKey = ParamKey.of(attrName);
                    FeatureParam<?> param = parseMsgFeatureParam(paramKey, value);
                    if (param != null) {
                        params.add(new FeatureParam<String>(paramKey, value));
                    }
                }
            }
            FeatureSpec<Message> feature = new FeatureSpec<Message>(featureType);
            for (FeatureParam<?> param: params) {
                feature.addParam(param);
            }
            fs.addFeatureSpec(feature);
        }
        spec.setNumFeatures(fs.getNumFeatures());
        String description = getOptionalElement(node, E_DESCRIPTION, null);
        return new ClassifierData<Message>(ClassifiableType.MESSAGE, name, spec, fs, description);
    }

    private static FeatureParam<?> parseMsgFeatureParam(ParamKey key, String value) throws ServiceException {
        switch (key) {
        case FREQUENCY_TYPE:
            try {
                ContactFrequencyEventType type = ContactFrequencyEventType.valueOf(value);
                return new FeatureParam<ContactFrequencyEventType>(key, type);
            } catch (IllegalArgumentException e) {
                throw ServiceException.INVALID_REQUEST("invalid ContactFrequencyEventType value " + value, null);
            }
        case METRIC_INITIALIZER:
            throw ServiceException.INVALID_REQUEST("cannot specify metric initializer", null);
        case RECIPIENT_TYPE:
            return new FeatureParam<RecipientCountType>(key, RecipientCountType.of(value));
        case TIME_RANGE:
            try {
                ContactFrequencyTimeRange timeRange = ContactFrequencyTimeRange.valueOf(value);
                return new FeatureParam<ContactFrequencyTimeRange>(key, timeRange);
            } catch (IllegalArgumentException e) {
                throw ServiceException.INVALID_REQUEST("invalid ContactFrequencyTimeRange value " + value, null);
            }
        case GLOBAL_RATIO:
            boolean bool;
            try {
                bool = BooleanUtils.toBoolean(value.toLowerCase(), "true", "false");
            } catch (IllegalArgumentException e) {
                try {
                    bool = BooleanUtils.toBoolean(value, "1", "0");
                } catch (IllegalArgumentException e2) {
                    throw ServiceException.INVALID_REQUEST("GLOBAL_RATIO value must be a boolean", null);
                }
            }
            return new FeatureParam<Boolean>(key, bool);
        case FROM_EVENT:
        case TO_EVENT:
        case NUMERATOR:
        case DENOMINATOR:
            return new FeatureParam<EventType>(key, EventType.of(value));
        default:
            break;
        }
        return null;
    }
}

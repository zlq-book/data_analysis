package com.travelsky.dataplatform.utils;

import org.jaxen.JaxenException;
import org.jaxen.dom.DOMXPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPathExpressionException;
import java.util.*;


/**
 * @author zcl
 */

public class XpathUtils {

    private static final Logger cat = LoggerFactory.getLogger(XpathUtils.class);

    private static final String NAMESPACE_PREFIX = "ota";
    private static final String NAMESPACE_URI = "http://www.opentravel.org/OTA/2003/05";

    private static Xpath createXpath(Object context, String expression, String namespacePrefix, String namespaceuri) {
        Xpath xPath = new Xpath(context, expression);

        if (namespacePrefix != null && namespaceuri != null) {
            xPath.addNamespace(namespacePrefix, namespaceuri);
        }
        return xPath;
    }

    public static String getString(Object context, String expression) {
        return getString(context, expression, NAMESPACE_PREFIX, NAMESPACE_URI);
    }

    private static String getString(Object context, String expression, String namespacePrefix, String namespaceuri) {
        Xpath xPath = createXpath(context, expression, namespacePrefix, namespaceuri);
        try {
            return xPath.getString();
        } catch (Exception e) {
            cat.error("Error applying xpath expression " + expression + ": " + e.getMessage(), e);
            return "";
        }
    }

    public static Node getNode(Object context, String expression) {
        return getNode(context, expression, NAMESPACE_PREFIX, NAMESPACE_URI);
    }

    private static Node getNode(Object context, String expression, String namespacePrefix, String namespaceuri) {
        Xpath xPath = createXpath(context, expression, namespacePrefix, namespaceuri);
        try {
            return xPath.getNode();
        } catch (Exception e) {
            cat.error("Error applying xpath expression " + expression + ": " + e.getMessage(), e);
            return null;
        }
    }

    public static NodeList getNodeList(Object context, String expression) {
        return getNodeList(context, expression, NAMESPACE_PREFIX, NAMESPACE_URI);
    }

    private static NodeList getNodeList(Object context, String expression, String namespacePrefix, String namespaceuri) {
        Xpath xPath = createXpath(context, expression, namespacePrefix, namespaceuri);
        try {
            return xPath.getNodeList();
        } catch (Exception e) {
            cat.error("Error applying xpath expression " + expression + ": " + e.getMessage(), e);
            return null;
        }
    }

    public static Double getNumber(Object context, String expression) {
        return getNumber(context, expression, NAMESPACE_PREFIX, NAMESPACE_URI);
    }

    private static Double getNumber(Object context, String expression, String namespacePrefix, String namespaceuri) {
        Xpath xPath = createXpath(context, expression, namespacePrefix, namespaceuri);
        try {
            Double returnVal = xPath.getNumber();
            if (returnVal.isNaN()) {
                return null;
            }
            return returnVal;
        } catch (Exception e) {
            cat.error("Error applying xpath expression " + expression + ": " + e.getMessage(), e);
            return null;
        }
    }

    public static int getInt(Object context, String expression) {
        return getInt(context, expression, NAMESPACE_PREFIX, NAMESPACE_URI);
    }

    private static int getInt(Object context, String expression, String namespacePrefix, String namespaceuri) {
        String str = getString(context, expression, namespacePrefix, namespaceuri);

        if (str.length() == 0) {
            return 0;
        }

        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            cat.error("Error apply xpath expression " + expression + ": " + e.getMessage(), e);
            return 0;
        }
    }

    public static Integer getInteger(Object context, String expression) {
        return getInteger(context, expression, NAMESPACE_PREFIX, NAMESPACE_URI);
    }

    private static Integer getInteger(Object context, String expression, String namespacePrefix, String namespaceuri) {
        String str = getString(context, expression, namespacePrefix, namespaceuri);

        if (str.length() == 0) {
            return null;
        }

        try {
            return Integer.valueOf(str);
        } catch (Exception e) {
            cat.error("Error apply xpath expression " + expression + ": " + e.getMessage(), e);
            return null;
        }
    }

    public static boolean getBoolean(Object context, String expression) {
        return getBoolean(context, expression, NAMESPACE_PREFIX, NAMESPACE_URI);
    }

    private static boolean getBoolean(Object context, String expression, String namespacePrefix, String namespaceuri) {
        Xpath xPath = createXpath(context, expression, namespacePrefix, namespaceuri);
        try {
            return xPath.getBoolean();
        } catch (Exception e) {
            cat.error("Error applying xpath expression " + expression + ": " + e.getMessage(), e);
            return false;
        }
    }

    static class Xpath implements NamespaceContext {

        private static final Logger cat = LoggerFactory.getLogger(Xpath.class);

        private DOMXPath domxPath;
        private Object context;
        private Map<String, String> uris = new HashMap<>();
        private Map<String, List<String>> prefixes = new HashMap<>();

        Xpath(Object context, String expression) {
            this.context = context;
            try {
                this.domxPath = new DOMXPath(expression);
            } catch (JaxenException e) {
                cat.error(e.getMessage(), e);
            }
        }

        void addNamespace(String prefix, String uri) {
            this.uris.put(prefix, uri);

            List<String> list = this.prefixes.get(uri);
            if (list == null) {
                this.prefixes.put(uri, list = new ArrayList());
            }
            list.add(prefix);

            if (this.domxPath != null) {
                try {
                    this.domxPath.addNamespace(prefix, uri);
                } catch (JaxenException e) {
                    cat.error(e.getMessage(), e);
                }
            }
        }

        @Override
        public String getNamespaceURI(String prefix) {
            return this.uris.get(prefix);
        }

        @Override
        public String getPrefix(String namespaceuri) {
            ArrayList prefixes = (ArrayList) this.prefixes.get(namespaceuri);
            if (prefixes == null) {
                return null;
            }
            if (prefixes.size() == 0) {
                return null;
            }
            return (String) prefixes.get(0);
        }

        @Override
        public Iterator getPrefixes(String namespaceuri) {
            ArrayList prefixes = (ArrayList) this.prefixes.get(namespaceuri);
            if (prefixes == null) {
                return null;
            }
            return prefixes.iterator();
        }

        NodeList getNodeList() throws XPathExpressionException {
            if (this.domxPath != null) {
                try {
                    final List nodes = this.domxPath.selectNodes(this.context);

                    return new NodeList() {
                        private List<Node> list = nodes;

                        @Override
                        public Node item(int index) {
                            if (index >= this.list.size()) {
                                return null;
                            }
                            return this.list.get(index);
                        }

                        @Override
                        public int getLength() {
                            return this.list.size();
                        }
                    };
                } catch (JaxenException e) {
                    throw new XPathExpressionException(e);
                }
            }
            return null;
        }

        Node getNode() throws XPathExpressionException {
            if (this.domxPath != null) {
                try {
                    return (Node) this.domxPath.selectSingleNode(this.context);
                } catch (JaxenException e) {
                    throw new XPathExpressionException(e);
                }
            }
            return null;
        }

        public String getString() throws XPathExpressionException {
            if (this.domxPath != null) {
                try {
                    return this.domxPath.stringValueOf(this.context);
                } catch (JaxenException e) {
                    throw new XPathExpressionException(e);
                }
            }

            return null;
        }

        Double getNumber() throws XPathExpressionException {
            if (this.domxPath != null) {
                try {
                    Number value = this.domxPath.numberValueOf(this.context);
                    return value.doubleValue();
                } catch (Exception e) {
                    throw new XPathExpressionException(e);
                }
            }
            // 返回NaN
            return Double.NaN;
        }

        public boolean getBoolean() throws XPathExpressionException {
            if (this.domxPath != null) {
                try {
                    return this.domxPath.booleanValueOf(this.context);
                } catch (JaxenException e) {
                    throw new XPathExpressionException(e);
                }
            }
            return false;
        }
    }
}


// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.galagosearch.tupleflow.Parameters;

/**
 * Valid query language syntax:
 *
 * #operator:argument(...)
 * term, or term.field, or term.field.field, etc.
 *
 * @author trevor
 */
public class StructuredQuery {
    /**
     * Copies a query tree using a traversal object.
     *
     * Both traversal.beforeNode and traversal.afterNode will be called
     * for every Node object in the query tree.
     * The traversal.beforeNode method will be called with a parent node
     * before any of its children (pre-order), while traversal.afterNode method
     * will be called on the parent after all of its children (post-order).
     *
     * The afterNode method will be called with a new copy of the original
     * node, with the children replaced by new copies.  afterNode can either
     * return its parameter or a modified node.
     */
    public static Node copy(Traversal traversal, Node tree) throws Exception {
        ArrayList<Node> children = new ArrayList<Node>();
        traversal.beforeNode(tree);

        for (Node n : tree.getInternalNodes()) {
            Node child = copy(traversal, n);
            children.add(child);
        }

        Node newNode = new Node(tree.getOperator(), tree.getParameters(),
                children, tree.getPosition());
        return traversal.afterNode(newNode);
    }

    /**
     * Walks a query tree with a traversal object.
     *
     * Both traversal.beforeNode and traversal.afterNode will be called
     * for every Node object in the query tree.
     * The traversal.beforeNode method will be called with a parent node
     * before any of its children (pre-order), while traversal.afterNode method
     * will be called on the parent after all of its children (post-order).
     */
    public static void walk(Traversal traversal, Node tree) throws Exception {
        traversal.beforeNode(tree);
        for (Node n : tree.getInternalNodes()) {
            walk(traversal, n);
        }
        traversal.afterNode(tree);
    }

    /**
     * Splits an input string, which may include escapes,
     * into chunks based on a delimiter character.  It does not split
     * on delimiters that appear in escaped regions.
     */
    public static ArrayList<String> splitOn(String text, char delimiter) {
        int start = 0;
        ArrayList<String> result = new ArrayList<String>();

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);

            if (c == '@') {
                i = StructuredQuery.findEscapedEnd(text, i);
            } else if (c == delimiter) {
                result.add(text.substring(start, i));
                start = i + 1;
            }
        }

        if (start != text.length()) {
            result.add(text.substring(start));
        }

        return result;
    }

    /**
     * <p>Parses an operator from the string query.  This method assumes that
     * operator is a pound sign ('#') followed by some text, followed by an open 
     * parentheses.  Parsing stops at the parenthesis.</p>
     *
     * <p>Note that parsing starts at index 0, not at "offset".  The offset is used purely
     * for giving parse error information, and represents the offset of the operator string
     * in the larger query string.</p>
     */
    public static Node parseOperator(String operator, int offset) {
        assert operator.charAt(0) == '#';
        int firstParen = operator.indexOf('(');

        if (firstParen < 0) {
            return new Node("unknown", new ArrayList<Node>(), offset);
        }

        String operatorText = operator.substring(0, firstParen);
        ArrayList<String> operatorParts = splitOn(operatorText.substring(1), ':');

        String operatorName = operatorParts.get(0);
        Parameters parameters = new Parameters();

        for (String part : operatorParts.subList(1, operatorParts.size())) {
            ArrayList<String> keyValue = splitOn(part, '=');

            if (keyValue.size() == 1) {
                parameters.add("default", decodeEscapes(part));
            } else if (keyValue.size() > 1) {
                String key = keyValue.get(0);
                String value = keyValue.get(1);
                parameters.add(decodeEscapes(key), decodeEscapes(value));
            }
        }

        int endOperator = operator.length();

        if (operator.charAt(operator.length() - 1) == ')') {
            endOperator--;
        }

        ArrayList<Node> children = parseArguments(operator.substring(firstParen + 1, endOperator),
                                                  offset + firstParen + 1);
        Node result = new Node(operatorName, parameters, children, offset);
        return result;
    }

    /**
     * Find the end of an escaped query region.
     * We assume that query.charAt(start) == '@'.  The
     * next charater is the boundary symbol for the escaped text.
     * We move forward in the string until we see the boundary symbol again.
     * If the escaped region isn't well formed (that is, there is no 
     * initial boundary symbol, or we only see the boundary symbol once),
     * query.length() is returned.
     */
    public static int findEscapedEnd(String query, int start) {
        assert query.charAt(start) == '@';

        // guard against the error case
        if (query.length() < start + 2) {
            return query.length();
        }
        char doneChar = query.charAt(start + 1);
        int result = query.indexOf(doneChar, start + 2);

        if (result < 0) {
            return query.length();
        }
        return result;
    }

    /**
     * Find the end of an operator.  We assume that query.charAt(start)
     * is a pound sign.  We skip forward in the query looking for
     * the end of the operator by looking at parentheses; we know we're
     * done when the parentheses are balanced and we've seen at least
     * one open parenthesis.  This method skips over escaped regions.
     */
    public static int findOperatorEnd(String query, int start) {
        int i = start;
        int open = 0;
        int closed = 0;

        for (; i < query.length() && (open != closed || open == 0); i++) {
            char current = query.charAt(i);

            if (current == '(') {
                open++;
            } else if (current == ')') {
                closed++;
            } else if (current == '@') {
                i = findEscapedEnd(query, i);
            }
        }

        return i;
    }

    public static int findTermEnd(String query, int start) {
        int i = start;

        for (; i < query.length(); i++) {
            char current = query.charAt(i);

            if (Character.isWhitespace(current)) {
                break;
            } else if (current == '@') {
                i = findEscapedEnd(query, i);
            }
        }

        return i;
    }

    public static String decodeEscapes(String escapedString) {
        StringBuilder builder = new StringBuilder();
        char escapeChar = ' ';
        boolean inEscape = false;

        for (int i = 0; i < escapedString.length(); i++) {
            char current = escapedString.charAt(i);

            if (!inEscape && current == '@' && i + 1 < escapedString.length()) {
                escapeChar = escapedString.charAt(i + 1);
                inEscape = true;
                i += 1;
            } else if (inEscape && current == escapeChar) {
                inEscape = false;
            } else {
                builder.append(escapedString.charAt(i));
            }
        }

        return builder.toString();
    }

    public static ArrayList<String> splitStringRespectingEscapes(String query, char split) {
        ArrayList<String> chunks = new ArrayList<String>();
        int start = 0;

        for (int i = 0; i < query.length(); i++) {
            char current = query.charAt(i);

            if (current == split) {
                chunks.add(query.substring(start, i));
                start = i + 1;
            } else if (current == '@') {
                i = findEscapedEnd(query, i);
            }
        }

        if (start < query.length() || query.length() == 0) {
            chunks.add(query.substring(start));
        }

        return chunks;
    }
    
    public static Node fieldOrNode(ArrayList<String> fieldNames, int offset) {
        assert fieldNames.size() > 0 : "Can't make an or node with no fields";
        Node result;
        
        if (fieldNames.size() == 1) {
            result = new Node("field", fieldNames.get(0), offset);
        } else {
            ArrayList<Node> children = new ArrayList<Node>();
            
            for (int i = 0; i < fieldNames.size(); ++i) {
                children.add(new Node("field", fieldNames.get(i), offset));
            }
            
            result = new Node("extentor", children, offset);
        }
        
        return result;
    }

    public static Node parseTerm(String query, int offset) {
        // step 1, split at periods
        ArrayList<String> chunks = splitStringRespectingEscapes(query, '.');
        
        // step 2, decode the chunks
        Node result = new Node("text", decodeEscapes(chunks.get(0)), offset);

        for (int i = 1; i < chunks.size(); i++) {
            ArrayList<Node> children = new ArrayList<Node>();
            children.add(result);
            String fieldText = chunks.get(i);
            boolean isSmoothingField = false;

            if (fieldText.startsWith("(") && fieldText.endsWith(")")) {
                isSmoothingField = true;
                fieldText = fieldText.substring(1, fieldText.length() - 1);
            }

            ArrayList<String> fieldNames = splitStringRespectingEscapes(fieldText, ',');
            Node fieldNode = fieldOrNode(fieldNames, offset);
            children.add(fieldNode);
            
            if (isSmoothingField) {
                result = new Node("smoothlm", children, offset);
            } else {
                result = new Node("inside", children, offset);
            }
        }

        return result;
    }

    public static ArrayList<Node> parseArguments(String query, int offset) {
        ArrayList<Node> arguments = new ArrayList<Node>();
        int start = 0;
        boolean inElement = false;

        // scan the string, looking for tokens.  Tokens are either operators (#\w+(...)), words, or escaped.
        for (int i = 0; i < query.length(); i++) {
            char current = query.charAt(i);

            if (!Character.isWhitespace(current)) {
                if (current == '#') {
                    // this is an operator, so scan for balanced parentheses,
                    // not including escaped regions
                    int end = findOperatorEnd(query, i);
                    Node child = parseOperator(query.substring(i, end), offset + i);
                    arguments.add(child);
                    i = end;
                } else {
                    int end = findTermEnd(query, i);
                    Node child = parseTerm(query.substring(i, end), offset + i);
                    arguments.add(child);
                    i = end;
                }
            }
        }

        // we're at the end of the string
        if (inElement) {
            Node child = new Node("text", query.substring(start), offset + start);
            arguments.add(child);
        }

        return arguments;
    }

    public static Node parse(String query) {
        ArrayList<Node> arguments = parseArguments(query, 0);

        if (query.length() == 0) {
            return new Node("text", "");
        } else if (arguments.size() == 1) {
            return arguments.get(0);
        } else {
            return new Node("combine", arguments, 0);
        }
    }

    public static Set<String> findQueryTerms(Node queryTree) {
        HashSet<String> queryTerms = new HashSet<String>();

        if (queryTree.getOperator().equals("text")) {
            queryTerms.add(queryTree.getDefaultParameter());
        } else {
            for (Node child : queryTree.getInternalNodes()) {
                queryTerms.addAll(findQueryTerms(child));
            }
        }

        return queryTerms;
    }
}

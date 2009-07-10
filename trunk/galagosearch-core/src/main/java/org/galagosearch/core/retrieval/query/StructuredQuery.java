// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.galagosearch.core.retrieval.query.StructuredLexer.TokenStream;
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

    public static Parameters parseParameters(TokenStream tokens) {
        Parameters parameters = new Parameters();
        assert tokens.currentEquals(":");

        while (tokens.currentEquals(":")) {
            tokens.next();
            String key = tokens.current().text;
            tokens.next();

            if (tokens.currentEquals("=")) {
                tokens.next();

                if (tokens.hasCurrent()) {
                    String value = tokens.current().text;
                    parameters.add(key, value);
                    tokens.next();
                }
            } else {
                parameters.add("default", key);
            }
        }

        return parameters;
    }

    public static Node parseOperator(TokenStream tokens) {
        int position = tokens.current().position;
        assert tokens.currentEquals("#");
        tokens.next();

        String operatorName = tokens.current().text;
        tokens.next();
        Parameters parameters = new Parameters();

        if (tokens.currentEquals(":")) {
            parameters = parseParameters(tokens);
        }

        if (tokens.currentEquals("(")) {
            tokens.next();
        }

        ArrayList<Node> arguments = parseArgumentList(tokens);
        
        if (tokens.currentEquals(")")) {
            tokens.next();
        }

        return new Node(operatorName, parameters, arguments, position);
    }

    public static Node parseQuotedTerms(TokenStream tokens) {
        assert tokens.currentEquals("\"");
        ArrayList<Node> children = new ArrayList<Node>();
        int position = tokens.current().position;
        tokens.next();

        while (!tokens.currentEquals("\"") && tokens.hasCurrent()) {
            children.add(parseTerm(tokens));
        }

        if (tokens.currentEquals("\"")) {
            tokens.next();
        }

        return new Node("quote", children, position);
    }

    public static Node parseTerm(TokenStream tokens) {
        if (tokens.currentEquals("\"")) {
            return parseQuotedTerms(tokens);
        } else {
            Node node = new Node("text", tokens.current().text, tokens.current().position);
            tokens.next();
            return node;
        }
    }

    public static Node parseUnrestricted(TokenStream tokens) {
        if (tokens.currentEquals("#")) {
            return parseOperator(tokens);
        } else {
            return parseTerm(tokens);
        }
    }

    public static ArrayList<Node> parseFieldList(TokenStream tokens) {
        ArrayList<Node> nodes = new ArrayList<Node>();
        Node field = new Node("field", tokens.current().text, tokens.current().position);
        nodes.add(field);
        tokens.next();
        while (tokens.currentEquals(",")) {
            tokens.next();
            field = new Node("field", tokens.current().text, tokens.current().position);
            nodes.add(field);
            tokens.next();
        }
        return nodes;
    }

    public static Node nodeWithOptionalExtentOr(String operator, Node child, ArrayList<Node> orFields) {
        Node second = null;
        if (orFields.size() == 1) {
            second = orFields.get(0);
        } else {
            second = new Node("extentor", orFields);
        }
        ArrayList<Node> children = new ArrayList<Node>();
        children.add(child);
        children.add(second);
        return new Node(operator, children);
    }

    public static Node parseRestricted(TokenStream tokens) {
        Node node = parseUnrestricted(tokens);

        tokens.pushMark();
        while (tokens.hasCurrent() && tokens.currentEquals(".")) {
            tokens.next();

            if (tokens.currentEquals("(")) {
                // Not a restriction
                break;
            } else {
                ArrayList<Node> restrictNodes = parseFieldList(tokens);
                node = nodeWithOptionalExtentOr("inside", node, restrictNodes);
                // We successfully parsed this, so move the rewind marker
                tokens.resetMark();
            }
        }

        tokens.rewindToMark();
        return node;
    }

    public static Node parseArgument(TokenStream tokens) {
        Node node = parseRestricted(tokens);

        if (tokens.currentEquals(".")) {
            tokens.next();
            assert tokens.currentEquals("(");
            tokens.next();

            ArrayList<Node> smoothingNodes = parseFieldList(tokens);
            assert tokens.currentEquals(")");
            tokens.next();

            node = nodeWithOptionalExtentOr("smoothinside", node, smoothingNodes);
        }

        return node;
    }

    public static ArrayList<Node> parseArgumentList(TokenStream tokens) {
        ArrayList<Node> arguments = new ArrayList<Node>();
        while (tokens.hasCurrent()) {
            if (tokens.current().text.equals(")")) {
                break;
            } else {
                arguments.add(parseArgument(tokens));
            }
        }
        return arguments;
    }

    public static Node parse(String query) {
        StructuredLexer lexer = new StructuredLexer();
        ArrayList<StructuredLexer.Token> tokens;
        try {
            tokens = lexer.tokens(query);
        } catch(Exception e) {
            // TODO: fix this
            e.printStackTrace();
            return new Node("text", "");
        }
        TokenStream stream = new TokenStream(tokens);
        ArrayList<Node> arguments = parseArgumentList(stream);

        if (arguments.size() == 0) {
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

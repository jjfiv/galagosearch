// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

/**
 * <p>StructuredQuery.parse parses queries using pseudo-operators, like #text and #field, so
 * that <tt>a.b.</tt> becomes <tt>#inside( #text:a() #field:b() )</p>.  These pseudo-operators
 * are not supported by common index types.  This traversal renames <tt>#text</tt> and
 * <tt>#field</tt> to something sensible.</p>
 * 
 * @author trevor
 */
public class TextFieldRewriteTraversal implements Traversal {
    private StructuredIndex index;
    private englishStemmer stemmer;

    public TextFieldRewriteTraversal(Parameters parameters, StructuredRetrieval retrieval) {
        this.index = retrieval.getIndex();
        this.stemmer = new englishStemmer();
    }

    public void beforeNode(Node object) throws Exception {
        // do nothing
    }

    public Node transformedNode(Node original,
            String operatorName, String indexName) {
        Parameters parameters = original.getParameters().clone();
        parameters.add("part", indexName);
        return new Node(operatorName, parameters, original.getInternalNodes(), original.getPosition());
    }

    private Node stemmedNode(Node original) {
        Parameters parameters = original.getParameters().clone();
        parameters.add("part", "stemmedPostings");
        String term = parameters.get("default");
        stemmer.setCurrent(term);
        stemmer.stem();
        String stemmed = stemmer.getCurrent();
        parameters.set("default", stemmed);
        return new Node("extents", parameters, original.getInternalNodes(), original.getPosition());
    }

    public Node afterNode(Node original) throws Exception {
        String operator = original.getOperator();

        if (operator.equals("text")) {
            if (index.containsPart("stemmedPostings")) {
                return stemmedNode(original);
            } else if (index.containsPart("postings")) {
                return transformedNode(original, "extents", "postings");
            } else {
                return original;
            }
        } else if (operator.equals("field") || operator.equals("any")) {
            if (index.containsPart("extents")) {
                return transformedNode(original, "extents", "extents");
            } else {
                return original;
            }
        } else {
            return original;
        }
    }
}

// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.query.MalformedQueryException;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

/**
 * Transforms a #prms operator into a full expansion of the
 * PRM-S model. That means:
 *
 * Given `meg ryan war`, the output should be like:
 *
 * #combine(
 * #combine:0=0.407:1=0.382:2=0.187 ( meg.cast  meg.team  meg.title )
 * #combine:0=0.601:1=0.381:2=0.017 ( ryan.cast  ryan.team  ryan.title )
 * #combine:0=0.927:1=0.070:2=0.002 ( war.cast  war.team  war.title ))
 *
 * @author jykim
 */
public class PRMSTraversal implements Traversal {

    private int levels;
    String[] fieldList;
    Retrieval retrieval;

    public PRMSTraversal(Parameters parameters, Retrieval retrieval) {
        levels = 0;
        this.retrieval = retrieval;
    }

    public void beforeNode(Node original) throws Exception {
        levels++;
    }

    public Node afterNode(Node original) throws Exception {
        levels--;
        if (levels > 0) {
            return original;
        } else if (original.getOperator().equals("prms")) {

            // Fetch the field list parameter from the query
            fieldList = original.getParameters().get("fields").split(",");
            for (String field : fieldList) {
                System.err.println("field : " + field);
            }

            // Get the field length using 'all' operator
            Map<String, Long> fieldLengths = new HashMap<String, Long>();
            for (String field : fieldList) {
                Parameters par2 = new Parameters();
                par2.add("default", field);
                par2.add("part", "extents");
                Node all = new Node("all", par2, new ArrayList(), 0);
                long f_length = retrieval.xcount(all.toString());
                fieldLengths.put(field, f_length);
                System.err.println(field.toString() + " : " + f_length);
            }

            ArrayList<Node> children = original.getInternalNodes();
            ArrayList<Node> terms = new ArrayList<Node>();
            for (Node child : children) {
                ArrayList<Node> termFields = new ArrayList<Node>();
                Parameters weights = new Parameters(); int i = 0;
                for (String field : fieldList) {
                    Parameters par1 = new Parameters();
                    par1.add("default", child.getDefaultParameter());
                    par1.add("part", "postings");
                    Node extents1 = new Node("extents", par1, new ArrayList(), 0);

                    Parameters par2 = new Parameters();
                    par2.add("default", field);
                    par2.add("part", "extents");
                    Node extents2 = new Node("extents", par2, new ArrayList(), 0);

                    ArrayList<Node> pair = new ArrayList<Node>();
                    pair.add(extents1);
                    pair.add(extents2);
                    Node n_inside = new Node("inside", new Parameters(), pair, 0);
//
//                    Parameters par3 = new Parameters();
//                    par3.add("default", "dirichlet");
//                    par3.add("mu", "1000");
//                    ArrayList<Node> data = new ArrayList<Node>();
//                    data.add(n_inside);
//                    Node n_smoothed = new Node("feature", par3, data, n_inside.getPosition());

                    long f_term_field = retrieval.xcount(n_inside.toString());
                    double f_term_field_prob = (double)f_term_field / fieldLengths.get(field);
                    termFields.add(n_inside);
                    weights.set( Integer.toString(i) , Double.toString(f_term_field_prob));
                    i++;
                }
                Node termFieldNodes = new Node("combine", weights, termFields, 0);
                terms.add(termFieldNodes);
            }
            Node termNodes = new Node("combine", new Parameters(), terms, original.getPosition());

            return termNodes;
        } else {
            return original;
        }
    }
}

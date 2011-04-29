// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.galagosearch.core.index.geometric.GeometricIndex;
import org.galagosearch.core.index.mem.MemoryChecker;
import org.galagosearch.core.parse.DocumentSource;
import org.galagosearch.core.parse.IncrementalUniversalParser;
import org.galagosearch.core.parse.SequentialDocumentNumberer;
import org.galagosearch.core.parse.TagTokenizer;
import org.galagosearch.core.store.SnippetGenerator;
import org.galagosearch.core.tools.Search.SearchResult;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * 
 * @author sjh
 */
public class GeometricSearch extends Search {

    GeometricIndex geoIndex;
    IncrementalUniversalParser uniPar;
    DocumentSource docSource;

    public GeometricSearch(Parameters params) throws Exception {

        FakeParameters tupleflowParams = new FakeParameters(params);
        geoIndex = new GeometricIndex(tupleflowParams);

        MemoryChecker memCheck = new MemoryChecker();
        memCheck.setProcessor(geoIndex);
        SequentialDocumentNumberer docNum = new SequentialDocumentNumberer();
        docNum.setProcessor(memCheck);
        TagTokenizer tt = new TagTokenizer();
        tt.setProcessor(docNum);
        uniPar = new IncrementalUniversalParser(tupleflowParams);
        uniPar.setProcessor(tt);
//        Sorter<DocumentSplit> sort = new Sorter<DocumentSplit>(new DocumentSplit.FileIdOrder());
//        sort.setProcessor(uniPar);
        docSource = new DocumentSource(tupleflowParams);
        docSource.setProcessor(uniPar);

        this.store = getDocumentStore(params.list("corpus"));
        this.retrieval = geoIndex;
        this.generator = new SnippetGenerator();

        docSource.processAllSplits();
    }

    public void plus100() throws IOException {
        uniPar.increment();
    }

    private SearchResult run(String queryText) throws Exception {
        Parameters p = new Parameters();
        p.add("startAt", Integer.toString(0));
        p.add("resultCount", Integer.toString(8));
        p.get("queryType", "simple");

        SearchResult res = runQuery(queryText.toLowerCase(), p, true);
        return res;
    }

    @Override
    public void close() throws IOException {
        super.close();
        docSource.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            System.err.println("Needs some parameters -- output / input ?");
            return;
        }

        // handle --links and --stemming flags
        String[][] filtered = Utility.filterFlags(args);

        String[] flags = filtered[0];
        String[] nonFlags = filtered[1];
        String indexDir = nonFlags[0];

        File dir = new File(indexDir);
        dir.mkdir();


        Parameters p = new Parameters(flags);
        p.add("corpus", nonFlags[1]);
        p.add("filename", nonFlags[1]);
        p.add("shardDirectory", indexDir);
        p.set("distrib", "0");
        p.set("galagoTemp", "");
        p.set("indexBlockSize", "300"); // could use 50000
        p.set("radix", "3");
        p.get("mergeMode", "local");

        GeometricSearch search = new GeometricSearch(p);
        search.plus100();

        Scanner in = new Scanner(System.in);
        boolean loop = true;
        String input;
        while (loop) {
            input = in.nextLine();
            if (input.equals("a")) {
                search.plus100();
            }
            if (input.equals("q")) {
                SearchResult results = search.run("asbestos cancer");
                for (int i = 0; i < results.items.size(); i++) {
                    double score = results.items.get(i).score;
                    int rank = i + 1;

                    System.out.println("Q0 " + results.items.get(i).identifier + " " + rank + " " + results.items.get(i).score + " galago\n");
                }
            }

            if (input.equals("x")) {
                search.close();
                loop = false;
            }
        }

    }
}

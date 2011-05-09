// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.store;

import java.io.IOException;
import java.sql.SQLException;
import org.galagosearch.core.parse.Document;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author trevor
 */
public class SQLDocumentStoreWriter implements Processor<Document> {
    SQLDocumentStore documentStore;

    public SQLDocumentStoreWriter(TupleFlowParameters parameters) throws SQLException, ClassNotFoundException {
        documentStore = new SQLDocumentStore(parameters);
    }

    public void process(Document document) throws IOException {
        documentStore.add(document);
    }

    public void close() throws IOException {
        documentStore.close();
    }

    public Class<Document> getInputClass() {
        return Document.class;
    }
}

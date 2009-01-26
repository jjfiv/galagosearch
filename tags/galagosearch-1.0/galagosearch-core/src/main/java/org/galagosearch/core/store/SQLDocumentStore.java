// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.store;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import org.galagosearch.core.parse.Document;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author trevor
 */
public class SQLDocumentStore implements Collection<Document>, DocumentStore {
    Connection connection;
    PreparedStatement insertDocument;
    PreparedStatement insertMetadata;
    PreparedStatement selectDocument;
    PreparedStatement selectDocumentByID;
    PreparedStatement selectMetadata;
    PreparedStatement insertByMetadata;

    public SQLDocumentStore(TupleFlowParameters parameters) throws SQLException, ClassNotFoundException {
        this(parameters.getXML());
    }

    public SQLDocumentStore(Parameters parameters) throws SQLException, ClassNotFoundException {
        this(parameters.get("driverName"), parameters.get("databaseUrl"));
    }

    public SQLDocumentStore(String driverName, String databaseUrl) throws SQLException, ClassNotFoundException {
        connection = connect(driverName, databaseUrl);
        createStatements();
    }

    private void createStatements() throws SQLException {
        String sql;
        sql = "insert into documents(documentText)" +
                "               values(?)";
        insertDocument = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);

        sql = "insert into documentMetadata(documentID, metadataKey, metadataValue)" +
                "                      values(?, ?, ?)";
        insertMetadata = connection.prepareStatement(sql);

        sql = "select documents.documentID, documents.documentText                             " +
                "  from documents                                                                " +
                "  join documentMetadata on (documents.documentID = documentMetadata.documentID) " +
                " where documentMetadata.metadataKey = ?                                         " +
                "   and documentMetadata.metadataValue = ?                                       ";
        selectDocument = connection.prepareStatement(sql);

        sql = "select documents.documentText           " +
                "  from documents                        " +
                " where documentID = ?                   ";
        selectDocumentByID = connection.prepareStatement(sql);

        sql = "select metadataKey, metadataValue       " +
                "  from documentMetadata " +
                " where documentID = ?   ";

        selectMetadata = connection.prepareStatement(sql);

        sql = "insert into documentMetadata(documentID, metadataKey, metadataValue) " +
                "select documentID, ?, ?                     " +
                "  from documentMetadata                     " +
                " where metadataKey = ? and metadataValue = ?";

        insertByMetadata = connection.prepareStatement(sql);
    }

    private static void unconditionalUpdate(Connection c, String sql) {
        try {
            Statement s = c.createStatement();
            s.executeUpdate(sql);
            s.close();
        } catch (Exception e) {
        }
    }

    public static Connection connect(String driverName, String databaseUrl) throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        return DriverManager.getConnection(databaseUrl);
    }

    public static void dropDatabase(String driverName, String databaseUrl) throws SQLException, ClassNotFoundException {
        Connection connection = connect(driverName, databaseUrl);

        unconditionalUpdate(connection, "drop table documents");
        unconditionalUpdate(connection, "drop table documentMetadata");
    }

    public static void createDatabase(String driverName, String databaseUrl) throws SQLException, ClassNotFoundException {
        Connection connection = connect(driverName, databaseUrl);

        String documentsSql = "create table documents (" +
                "     documentID int PRIMARY KEY AUTO_INCREMENT, " +
                "     documentText LONGTEXT                      " +
                ") ";

        String metadataSql = "create table documentMetadata (   " +
                "     documentID int,               " +
                "     metadataKey varchar(50),      " +
                "     metadataValue varchar(1000)   " +
                ") ";

        unconditionalUpdate(connection, documentsSql);
        unconditionalUpdate(connection, metadataSql);
    }

    public void close() throws IOException {
        try {
            insertMetadata.close();
            insertDocument.close();
            connection.close();
        } catch (SQLException e) {
            throw new IOException("Caught exception while closing stuff.");
        }
    }

    public synchronized boolean add(Document document) {
        try {
            insertDocument.setString(1, document.text);
            insertDocument.executeUpdate();

            // got the key set
            ResultSet keySet = insertDocument.getGeneratedKeys();
            if (!keySet.next()) {
                return false;
            }
            long documentID = keySet.getInt(1);
            keySet.close();

            // now, add in rows for the metadata
            for (Entry<String, String> entry : document.metadata.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                insertMetadata.setLong(1, documentID);
                insertMetadata.setString(2, key);
                insertMetadata.setString(3, value);
                insertMetadata.executeUpdate();
                insertMetadata.clearParameters();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to add a document to the DocumentStore", e);
        }

        return true;
    }

    public synchronized void addMetadata(
            String oldKey, String oldValue,
            String newKey, String newValue) throws SQLException {
        insertByMetadata.setString(1, newKey);
        insertByMetadata.setString(2, newValue);

        insertByMetadata.setString(3, oldKey);
        insertByMetadata.setString(4, oldValue);

        insertByMetadata.execute();
    }

    public Iterator<Document> iterator() {
        final ResultSet rs;
        final boolean hn;
        final Statement statement;

        try {
            String sql = "select documents.documentID " +
                    "  from documents            ";
            statement = connection.createStatement();
            rs = statement.executeQuery(sql);
            hn = rs.next();

            if (hn == false) {
                rs.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't create an iterator for the DocumentStore", e);
        }

        return new Iterator<Document>() {
            ResultSet resultSet = rs;
            boolean hasNext = hn;

            public boolean hasNext() {
                return hasNext;
            }

            public Document next() {
                if (hasNext == false) {
                    return null;
                }
                Document document;

                try {
                    long documentID = resultSet.getLong(1);
                    document = get(documentID);
                    hasNext = resultSet.next();

                    if (hasNext == false) {
                        resultSet.close();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Couldn't get next Document from the database", e);
                }

                return document;
            }

            public void remove() {
                throw new UnsupportedOperationException(
                        "Removing documents is not supported by the DocumentStore iterator.");
            }
        };
    }

    public synchronized void addMetadata(long documentID, Document document) throws SQLException {
        selectMetadata.setLong(1, documentID);
        document.metadata = new HashMap();
        ResultSet metadata = selectMetadata.executeQuery();

        while (metadata.next()) {
            String key = metadata.getString(1);
            String value = metadata.getString(2);
            document.metadata.put(key, value);
        }

        metadata.close();
    }

    public synchronized Document get(String identifier) throws IOException {
        try {
            return get("identifier", identifier);
        } catch(SQLException e) {
            IOException exception = new IOException("Caught a SQLException");
            exception.initCause(e);
            throw exception;
        }
    }

    public synchronized Document get(long documentID) throws SQLException {
        Document result = new Document();
        selectDocumentByID.setLong(1, documentID);
        ResultSet document = selectDocumentByID.executeQuery();
        String text;

        if (document.next()) {
            text = document.getString(1);

            result.text = text;
            document.close();
        } else {
            document.close();
            return null;
        }

        addMetadata(documentID, result);
        return result;
    }

    public synchronized Document get(String metadataKey, String metadataValue) throws SQLException {
        Document result = new Document();

        selectDocument.setString(1, metadataKey);
        selectDocument.setString(2, metadataValue);
        ResultSet document = selectDocument.executeQuery();

        long documentID;
        String text;

        if (document.next()) {
            documentID = document.getLong(1);
            text = document.getString(2);

            result.text = text;
            document.close();
        } else {
            document.close();
            return null;
        }

        addMetadata(documentID, result);
        return result;
    }

    public int size() {
        ResultSet resultSet = null;
        Statement statement = null;
        int size = 0;

        try {
            statement = connection.createStatement();
            String sql = "select count(*) from documents";
            resultSet = statement.executeQuery(sql);

            if (resultSet.next()) {
                size = resultSet.getInt(1);
                resultSet.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException("Size operation failed", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                // ignore this exception
            }
        }

        return size;
    }

    public boolean isEmpty() {
        return size() != 0;
    }

    public boolean contains(Document d) {
        throw new RuntimeException("contains() is not supported by SQLDocumentStore");
    }

    public Document[] toArray() {
        return toArray(new Document[0]);
    }

    public <T> T[] toArray(T[] example) {
        ArrayList<Document> list = new ArrayList();

        for (Document d : this) {
            list.add(d);
        }

        return list.toArray(example);
    }

    public boolean remove(Object o) {
        throw new RuntimeException("remove() is not supported by SQLDocumentStore");
    }

    public boolean contains(Object o) {
        throw new RuntimeException("contains() is not supported by SQLDocumentStore");
    }

    public boolean containsAll(Collection<?> documents) {
        boolean doesContain = true;
        for (Object d : documents) {
            if (!contains(d)) {
                return false;
            }
        }
        return true;
    }

    public boolean addAll(Collection<? extends Document> documents) {
        for (Object d : documents) {
            if (d instanceof Document) {
                add((Document) d);
            } else {
                return false;
            }
        }
        return true;
    }

    public boolean removeAll(Collection<?> objects) {
        for (Object o : objects) {
            remove(o);
        }
        return true;
    }

    public boolean retainAll(Collection<?> objects) {
        throw new RuntimeException("retainAll() is not supported by SQLDocumentStore");
    }

    public void clear() {
        throw new RuntimeException("clear() is not supported by SQLDocumentStore");
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql:///document_store?user=root";

        SQLDocumentStore.dropDatabase(driver, url);
        SQLDocumentStore.createDatabase(driver, url);

        SQLDocumentStore s = new SQLDocumentStore(driver, url);

        Document document = new Document();
        document.identifier = "WTX000-000-00";
        document.metadata.put("identifier", document.identifier);
        document.metadata.put("hi", "mom");
        document.text = "hello!  hello!";
        s.add(document);

        s.addMetadata("hi", "mom", "initial", "test");
        Document d = s.get("hi", "mom");

        for (Document e : s) {
            System.out.println(e.text);
        }

        s.close();
    }
}

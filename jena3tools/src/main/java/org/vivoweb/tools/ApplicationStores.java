package org.vivoweb.tools;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sdb.SDBFactory;
import org.apache.jena.sdb.Store;
import org.apache.jena.sdb.StoreDesc;
import org.apache.jena.sdb.layout2.ValueType;
import org.apache.jena.sdb.sql.SDBConnection;
import org.apache.jena.sdb.sql.SDBExceptionSQL;
import org.apache.jena.sdb.store.DatabaseType;
import org.apache.jena.sdb.store.LayoutType;
import org.apache.jena.sdb.util.StoreUtils;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.TDB;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.vocabulary.RDF;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class ApplicationStores {
    private final Model applicationModel;

    private Dataset contentDataset;
    private Dataset configurationDataset;

    private Connection contentConnection;
    private StoreDesc  contentStoreDesc;

    private RDFFormat outputFormat;

    private boolean configured = false;

    public ApplicationStores(String homeDir, RDFFormat outputFormat) {

        File config = Utils.resolveFile(homeDir, "config/applicationSetup.n3");

        this.outputFormat = outputFormat;

        try {
            InputStream in = new FileInputStream(config);
            applicationModel = ModelFactory.createDefaultModel();
            applicationModel.read(in, null, "N3");
            in.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Application setup not found");
        } catch (IOException e) {
            throw new RuntimeException("Error closing config", e);
        }

        try {
            Resource contentSource = getObjectFor("http://vitro.mannlib.cornell.edu/ns/vitro/ApplicationSetup#hasContentTripleSource");
            Resource configurationSource = getObjectFor("http://vitro.mannlib.cornell.edu/ns/vitro/ApplicationSetup#hasConfigurationTripleSource");

            if (isType(contentSource, "java:edu.cornell.mannlib.vitro.webapp.triplesource.impl.sdb.ContentTripleSourceSDB")) {
                Properties props = new Properties();
                try {
                    InputStream in = new FileInputStream(Utils.resolveFile(homeDir, "runtime.properties"));
                    props.load(in);
                    in.close();
                } catch (FileNotFoundException f) {
                    throw new RuntimeException("Unable to find properties");
                } catch (IOException e) {
                    throw new RuntimeException("Unable to load properties", e);
                }

                contentConnection = makeConnection(props);
                contentStoreDesc  = makeStoreDesc(props);
                Store store = SDBFactory.connectStore(contentConnection, contentStoreDesc);
                if (store == null) {
                    throw new RuntimeException("Unable to connect to SDB content triple store");
                }

                if (!(StoreUtils.isFormatted(store))) {
                    store.getTableFormatter().create();
                    store.getTableFormatter().truncate();
                }

                contentDataset = SDBFactory.connectDataset(store);
                if (contentDataset == null) {
                    throw new RuntimeException("Unable to connect to SDB content dataset");
                }
            } else if (isType(contentSource, "java:edu.cornell.mannlib.vitro.webapp.triplesource.impl.tdb.ContentTripleSourceTDB")) {
                Statement stmt = contentSource.getProperty(applicationModel.createProperty("http://vitro.mannlib.cornell.edu/ns/vitro/ApplicationSetup#hasTdbDirectory"));

                String tdbDirectory = null;
                if (stmt != null) {
                    tdbDirectory = stmt.getObject().asLiteral().getString();
                }

                if (StringUtils.isEmpty(tdbDirectory)) {
                    throw new RuntimeException("Content Source TDB missing directory property");
                }

                File contentFile = Utils.resolveFile(homeDir, tdbDirectory);
                if (!contentFile.exists()) {
                    if (!contentFile.mkdirs()) {
                        throw new RuntimeException("Unable to create content TDB source " + contentFile.getAbsolutePath());
                    }
                } else if (!contentFile.isDirectory()) {
                    throw new RuntimeException("Content triple source exists but is not a directory " + contentFile.getAbsolutePath());
                }

                contentDataset = TDBFactory.createDataset(contentFile.getAbsolutePath());
                if (contentDataset == null) {
                    throw new RuntimeException("Unable to open TDB content triple store");
                }
            }

            if (isType(configurationSource, "java:edu.cornell.mannlib.vitro.webapp.triplesource.impl.tdb.ConfigurationTripleSourceTDB")) {
                File configFile = Utils.resolveFile(homeDir, "tdbModels");
                if (!configFile.exists()) {
                    if (!configFile.mkdirs()) {
                        throw new RuntimeException("Unable to create configuration source " + configFile.getAbsolutePath());
                    }
                } else if (!configFile.isDirectory()) {
                    throw new RuntimeException("Configuration triple source exists but is not a directory " + configFile.getAbsolutePath());
                }

                configurationDataset = TDBFactory.createDataset(configFile.getAbsolutePath());
                if (configurationDataset == null) {
                    throw new RuntimeException("Unable to open TDB configuration triple store");
                }
            }

            configured = true;
        } catch (SQLException e) {
            throw new RuntimeException("SQL Exception", e);
        } finally {
            if (!configured) {
                close();
            }
        }
    }

    public void readConfiguration(File input) {
        if (configurationDataset != null) {
            try {
                InputStream inputStream = new BufferedInputStream(new FileInputStream(input));
                try {
                    RDFDataMgr.read(configurationDataset, inputStream, Lang.TRIG);
                    TDB.sync(configurationDataset);
                } finally {
                    inputStream.close();
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Unable to find configuration dump");
            } catch (IOException e) {
                throw new RuntimeException("Unable to read configuration dump", e);
            }
        }
    }

    public void readContent(File input) {
        if (contentDataset != null) {
            try {
                InputStream inputStream = new BufferedInputStream(new FileInputStream(input));
                try {
                    if (contentConnection != null) {
                        contentConnection.setAutoCommit(false);
                        RDFDataMgr.read(contentDataset, inputStream, Lang.TRIG);
                        contentConnection.commit();
                    } else {
                        RDFDataMgr.read(contentDataset, inputStream, Lang.TRIG);
                        TDB.sync(contentDataset);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("SQL exception", e);
                } finally {
                    inputStream.close();
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Unable to find content dump (dir error)");
            } catch (IOException e) {
                throw new RuntimeException("Unable to read content dump", e);
            }
        }
    }

    public void writeConfiguration(File output) {
        if (configurationDataset != null) {
            try {
                OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(output, false));
                writeRDF(outputStream, configurationDataset, outputFormat);
                outputStream.close();
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Unable to write configuration dump (dir error)");
            } catch (IOException e) {
                throw new RuntimeException("Unable to write configuration dump", e);
            }
        }
    }

    public void writeContent(File output) {
        if (contentDataset != null) {
            try {
                OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(output, false));
                try {
                    if (LayoutType.LayoutTripleNodesHash.equals(contentStoreDesc.getLayout()) ||
                        LayoutType.LayoutTripleNodesIndex.equals(contentStoreDesc.getLayout())) {
                        if (DatabaseType.MySQL.equals(contentStoreDesc.getDbType()) ||
                                DatabaseType.PostgreSQL.equals(contentStoreDesc.getDbType())) {

                            long offset = 0;
                            long limit  = 10000;

                            Dataset blankQuads = DatasetFactory.create();

                            while (writeContentSQL(outputStream, blankQuads, offset, limit)) {
                                offset += limit;
                            }

                            if (blankQuads.asDatasetGraph().size() > 0) {
                                writeRDF(outputStream, blankQuads, outputFormat);
                            }
                        } else {
                            writeRDF(outputStream, contentDataset, outputFormat);
                        }
                    } else {
                        writeRDF(outputStream, contentDataset, outputFormat);
                    }
                } finally {
                    outputStream.close();
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Unable to write content dump (dir error)");
            } catch (IOException e) {
                throw new RuntimeException("Unable to write content dump", e);
            }
        }
    }

    private boolean writeContentSQL(OutputStream outputStream, Dataset blankQuads, long offset, long limit) {
        Dataset quads = DatasetFactory.create();

        try {
            java.sql.Statement stmt = contentConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT \n" +
                    "N1.lex AS s_lex, N1.lang AS s_lang, N1.datatype AS s_datatype, N1.type AS s_type,\n" +
                    "N2.lex AS p_lex, N2.lang AS p_lang, N2.datatype AS p_datatype, N2.type AS p_type,\n" +
                    "N3.lex AS o_lex, N3.lang AS o_lang, N3.datatype AS o_datatype, N3.type AS o_type,\n" +
                    "N4.lex AS g_lex, N4.lang AS g_lang, N4.datatype AS g_datatype, N4.type AS g_type \n" +
                    "FROM\n" +
                    "(SELECT g,s,p,o FROM Quads" +
                    " ORDER BY g,s,p,o " +
                    (limit > 0 ? "LIMIT " + limit : "") +
                    (offset > 0 ? " OFFSET " + offset : "") + ") Q\n" +
                    ( LayoutType.LayoutTripleNodesHash.equals(contentStoreDesc.getLayout()) ?
                            (
                                    "LEFT OUTER JOIN Nodes AS N1 ON ( Q.s = N1.hash ) " +
                                    "LEFT OUTER JOIN Nodes AS N2 ON ( Q.p = N2.hash ) " +
                                    "LEFT OUTER JOIN Nodes AS N3 ON ( Q.o = N3.hash ) " +
                                    "LEFT OUTER JOIN Nodes AS N4 ON ( Q.g = N4.hash ) "
                            ) :
                            (
                                    "LEFT OUTER JOIN Nodes AS N1 ON ( Q.s = N1.id ) " +
                                    "LEFT OUTER JOIN Nodes AS N2 ON ( Q.p = N2.id ) " +
                                    "LEFT OUTER JOIN Nodes AS N3 ON ( Q.o = N3.id ) " +
                                    "LEFT OUTER JOIN Nodes AS N4 ON ( Q.g = N4.id ) "
                            )
                    )
            );

            try {
                while (rs.next()) {
                    Node subjectNode = makeNode(
                            rs.getString("s_lex"),
                            rs.getString("s_datatype"),
                            rs.getString("s_lang"),
                            ValueType.lookup(rs.getInt("s_type")));

                    Node predicateNode = makeNode(
                            rs.getString("p_lex"),
                            rs.getString("p_datatype"),
                            rs.getString("p_lang"),
                            ValueType.lookup(rs.getInt("p_type")));

                    Node objectNode = makeNode(
                            rs.getString("o_lex"),
                            rs.getString("o_datatype"),
                            rs.getString("o_lang"),
                            ValueType.lookup(rs.getInt("o_type")));

                    Node graphNode = makeNode(
                            rs.getString("g_lex"),
                            rs.getString("g_datatype"),
                            rs.getString("g_lang"),
                            ValueType.lookup(rs.getInt("g_type")));

                    if (subjectNode.isBlank() || predicateNode.isBlank() || objectNode.isBlank()) {
                        blankQuads.asDatasetGraph().add(Quad.create(
                                graphNode,
                                Triple.create(subjectNode, predicateNode, objectNode)
                        ));
                    } else {
                        quads.asDatasetGraph().add(Quad.create(
                                graphNode,
                                Triple.create(subjectNode, predicateNode, objectNode)
                        ));
                    }
                }
            } finally {
                rs.close();
            }

            if (quads.asDatasetGraph().size() > 0) {
                writeRDF(outputStream, quads, outputFormat);
                return true;
            }
        } catch (SQLException sqle) {
            throw new RuntimeException("Unable to retrieve triples", sqle);
        } finally {
        }

        return false;
    }

    // Copied from Jena SQLBridge2
    private static Node makeNode(String lex, String datatype, String lang, ValueType vType) {
        switch(vType) {
            case BNODE:
                return NodeFactory.createBlankNode(lex);
            case URI:
                return NodeFactory.createURI(lex);
            case STRING:
                return NodeFactory.createLiteral(lex, lang);
            case XSDSTRING:
                return NodeFactory.createLiteral(lex, XSDDatatype.XSDstring);
            case INTEGER:
                return NodeFactory.createLiteral(lex, XSDDatatype.XSDinteger);
            case DOUBLE:
                return NodeFactory.createLiteral(lex, XSDDatatype.XSDdouble);
            case DATETIME:
                return NodeFactory.createLiteral(lex, XSDDatatype.XSDdateTime);
            case OTHER:
                RDFDatatype dt = TypeMapper.getInstance().getSafeTypeByName(datatype);
                return NodeFactory.createLiteral(lex, dt);
            default:
                return NodeFactory.createLiteral("UNRECOGNIZED");
        }
    }

    private void writeRDF(OutputStream outputStream, Dataset dataset, RDFFormat outputFormat){
        if (outputFormat.equals(RDFFormat.NQ) || outputFormat.equals(RDFFormat.TRIG_BLOCKS) ||
                outputFormat.equals(RDFFormat.JSONLD)) {
            // for quad formats, write the dataset
            RDFDataMgr.write(outputStream, dataset, outputFormat);
        } else {
            // for triple formats, write the union model. Graph information is not included
            RDFDataMgr.write(outputStream, dataset.getUnionModel(), outputFormat);
        }
    }

    public boolean isEmpty() {
        boolean empty = true;

        try {
            if (configurationDataset != null) {
                empty &= configurationDataset.asDatasetGraph().isEmpty();
            }
        } catch (SDBExceptionSQL s) {

        }

        try {
            if (contentDataset != null) {
                empty &= contentDataset.asDatasetGraph().isEmpty();
            }
        } catch (SDBExceptionSQL s) {

        }

        return empty;
    }

    public boolean validateFiles(File configurationDump, File contentDump) {
        if (configurationDataset != null && !configurationDump.exists()) {
            return false;
        }

        if (contentDataset != null && !contentDump.exists()) {
            return false;
        }

        return true;
    }

    public void close() {
        if (configurationDataset != null) {
            configurationDataset.close();
        }

        if (contentDataset != null) {
            contentDataset.close();
        }

        TDB.closedown();
    }

    private boolean isType(Resource resource, String type) {
        return resource.hasProperty(RDF.type, applicationModel.createResource(type));
    }

    private Resource getObjectFor(String property) {
        NodeIterator iter = applicationModel.listObjectsOfProperty(
                applicationModel.createProperty(property)
        );

        try {
            while (iter.hasNext()) {
                RDFNode node = iter.next();
                if (node != null && node.isResource()) {
                    return node.asResource();
                }
            }
        } finally {
            iter.close();
        }

        return null;
    }

    // SDB

    private StoreDesc makeStoreDesc(Properties props) {
        String layoutStr = props.getProperty(PROPERTY_DB_SDB_LAYOUT, DEFAULT_LAYOUT).trim();
        String dbtypeStr = props.getProperty(PROPERTY_DB_TYPE, DEFAULT_TYPE).trim();
        return new StoreDesc(LayoutType.fetch(layoutStr), DatabaseType.fetch(dbtypeStr));
    }

    private Connection makeConnection(Properties props) {
        try {
            Class.forName(props.getProperty(PROPERTY_DB_DRIVER_CLASS_NAME, DEFAULT_DRIVER_CLASS).trim());
            String url = props.getProperty(PROPERTY_DB_URL).trim();
            String user = props.getProperty(PROPERTY_DB_USERNAME).trim();
            String pass = props.getProperty(PROPERTY_DB_PASSWORD).trim();

            String dbtypeStr = props.getProperty(PROPERTY_DB_TYPE, DEFAULT_TYPE).trim();
            if (DEFAULT_TYPE.equals(dbtypeStr)) {
                if (!url.contains("?")) {
                    url += "?useUnicode=yes&characterEncoding=utf8&nullNamePatternMatchesAll=true&cachePrepStmts=true&useServerPrepStmts=true&serverTimezone=UTC&useSSL=false";
                } else {
                    String urlLwr = url.toLowerCase();
                    if (!urlLwr.contains("useunicode")) {
                        url += "&useUnicode=yes";
                    }
                    if (!urlLwr.contains("characterencoding")) {
                        url += "&characterEncoding=utf8";
                    }
                    if (!urlLwr.contains("nullnamepatternmatchesall")) {
                        url += "&nullNamePatternMatchesAll=true";
                    }
                    if (!urlLwr.contains("cacheprepstmts")) {
                        url += "&cachePrepStmts=true";
                    }
                    if (!urlLwr.contains("useserverprepstmts")) {
                        url += "&useServerPrepStmts=true";
                    }
                    if (!urlLwr.contains("servertimezone")) {
                        url += "&serverTimezone=UTC";
                    }
                    if (!urlLwr.contains("usessl")) {
                        url += "&useSSL=false";
                    }
                }
            }
            
            return DriverManager.getConnection(url, user, pass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to find database driver");
        } catch (SQLException e) {
            throw new RuntimeException("Unable to create JDBC connection", e);
        }
    }

    static final String DEFAULT_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    static final String DEFAULT_LAYOUT = "layout2/hash";
    static final String DEFAULT_TYPE = "MySQL";

    static final String PROPERTY_DB_URL = "VitroConnection.DataSource.url";
    static final String PROPERTY_DB_USERNAME = "VitroConnection.DataSource.username";
    static final String PROPERTY_DB_PASSWORD = "VitroConnection.DataSource.password";
    static final String PROPERTY_DB_DRIVER_CLASS_NAME = "VitroConnection.DataSource.driver";
    static final String PROPERTY_DB_SDB_LAYOUT = "VitroConnection.DataSource.sdb.layout";
    static final String PROPERTY_DB_TYPE = "VitroConnection.DataSource.dbtype";
}

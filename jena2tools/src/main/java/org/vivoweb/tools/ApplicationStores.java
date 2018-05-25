package org.vivoweb.tools;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.sdb.layout2.ValueType;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.tdb.TDB;
import org.apache.commons.lang3.StringUtils;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.sdb.StoreDesc;
import com.hp.hpl.jena.sdb.sql.SDBExceptionSQL;
import com.hp.hpl.jena.sdb.store.DatabaseType;
import com.hp.hpl.jena.sdb.store.LayoutType;
import com.hp.hpl.jena.sdb.util.StoreUtils;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.vocabulary.RDF;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

        this.outputFormat = outputFormat;

        File config = Utils.resolveFile(homeDir, "config/applicationSetup.n3");

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

            // Check for content source type in both 1.10 and <1.9 formats
            if (isType(contentSource, "java:edu.cornell.mannlib.vitro.webapp.triplesource.impl.sdb.ContentTripleSourceSDB") ||
                    isType(contentSource, "java:edu.cornell.mannlib.vitro.webapp#triplesource.impl.sdb.ContentTripleSourceSDB")) {
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
                // Check for content source type in both 1.10 and <1.9 formats
            } else if (isType(contentSource, "java:edu.cornell.mannlib.vitro.webapp.triplesource.impl.tdb.ContentTripleSourceTDB") ||
                    isType(contentSource, "java:edu.cornell.mannlib.vitro.webapp#triplesource.impl.tdb.ContentTripleSourceTDB")) {
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
            } else {
                 throw new RuntimeException("No compatible 'contentSource' found!");
            }

            // Check for configuration source type in both 1.10 and <1.9 formats
            if (isType(configurationSource, "java:edu.cornell.mannlib.vitro.webapp.triplesource.impl.tdb.ConfigurationTripleSourceTDB") ||
                    isType(configurationSource, "java:edu.cornell.mannlib.vitro.webapp#triplesource.impl.tdb.ConfigurationTripleSourceTDB")) {
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
            } else {
                throw new RuntimeException("No compatible 'configurationSource' found!");
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
                if (outputFormat.equals(RDFFormat.NQ) || outputFormat.equals(RDFFormat.TRIG_BLOCKS)) {
                    // for quad formats, write the dataset
                    RDFDataMgr.write(outputStream, configurationDataset, outputFormat);
                } else {
                    // for triple formats, create a union model and write it
                    Model m = configurationDataset.getNamedModel("http://vitro.mannlib.cornell.edu/default/vitro-kb-displayMetadata");
                    m = m.union(configurationDataset.getNamedModel("http://vitro.mannlib.cornell.edu/default/vitro-kb-userAccounts"));
                    RDFDataMgr.write(outputStream, m, outputFormat);
                }
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

                            Dataset blankQuads = DatasetFactory.createMem();

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

    public Model createUnionModel(Dataset d) {
        Model m = ModelFactory.createDefaultModel();
        try{
            java.sql.Statement stmt = contentConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT N4.lex AS g_name FROM (SELECT g from Quads GROUP BY g " +
                    "ORDER BY g) Q LEFT OUTER JOIN Nodes AS N4 ON (Q.g = N4.hash)");
            try{
                while(rs.next()) {
                    System.out.println("Union model " + rs.getString("g_name"));
                    m = m.union(d.getNamedModel(rs.getString("g_name")));
                }
            }
            finally {
                rs.close();
            }
        }
        catch (SQLException sqle) {
            throw new RuntimeException("Unable to construct union model", sqle);
        }
        return m;
    }

    private boolean writeContentSQL(OutputStream outputStream, Dataset blankQuads, long offset, long limit) {
        Dataset quads = DatasetFactory.createMem();

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
                return NodeFactory.createAnon(new AnonId(lex));
            case URI:
                return NodeFactory.createURI(lex);
            case STRING:
              return NodeFactory.createLiteral(lex, lang, false);
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
        if (outputFormat.equals(RDFFormat.NQ) || outputFormat.equals(RDFFormat.TRIG_BLOCKS)) {
            // for quad formats, write the dataset
            RDFDataMgr.write(outputStream, dataset, outputFormat);
         } else {
            // for triple formats, create a union model and write it
            Model m = createUnionModel(dataset);
            RDFDataMgr.write(outputStream, m, outputFormat);
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
            if (DEFAULT_TYPE.equals(dbtypeStr) && !url.contains("?")) {
                url += "?useUnicode=yes&characterEncoding=utf8";
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

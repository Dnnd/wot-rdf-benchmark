package ru.agentlab.graphdb.utils;

import com.ontotext.trree.config.OWLIMSailSchema;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.base.RepositoryConnectionWrapper;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sail.Sail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;

public class EmbeddedGraphDB implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(EmbeddedGraphDB.class);
    private final InputStream repoConfiguration;
    private LocalRepositoryManager repositoryManager;
    private SailRepository repository;

    /**
     * Creates a new embedded instance of GraphDB in the provided directory.
     *
     * @param baseDir a directory where to store repositories
     * @throws RepositoryException
     */
    public EmbeddedGraphDB(File baseDir, InputStream repoConfiguration) throws RepositoryException {
        this.repoConfiguration = repoConfiguration;
        repositoryManager = new LocalRepositoryManager(baseDir);
        repositoryManager.init();
    }

    /**
     * Creates a repository with the given ID.
     *
     * @param repositoryId a new repository ID
     * @throws RDFHandlerException
     * @throws RepositoryConfigException
     * @throws RDFParseException
     * @throws IOException
     * @throws RepositoryException
     */
    public void createRepository(String repositoryId)
            throws RDFHandlerException, RepositoryConfigException, RDFParseException, IOException, RepositoryException {
        createRepository(repositoryId, null, null);
    }

    /**
     * Creates a repository with the given ID, label and optional override parameters.
     *
     * @param repositoryId    a new repository ID
     * @param repositoryLabel a repository label, or null if none should be set
     * @param overrides       a map of repository creation parameters that override the defaults, or null if none should be overridden
     * @throws RDFParseException
     * @throws IOException
     * @throws RDFHandlerException
     * @throws RepositoryConfigException
     * @throws RepositoryException
     */
    public void createRepository(String repositoryId, String repositoryLabel, Map<String, String> overrides)
            throws RDFParseException, IOException, RDFHandlerException,
            RepositoryConfigException, RepositoryException {

        if (repositoryManager.hasRepositoryConfig(repositoryId)) {
            throw new RuntimeException("Repository " + repositoryId + " already exists.");
        }

        TreeModel graph = new TreeModel();

        RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
        rdfParser.setRDFHandler(new StatementCollector(graph));
        rdfParser.parse(repoConfiguration, RepositoryConfigSchema.NAMESPACE);
        repoConfiguration.close();

        Resource repositoryNode = Models.subject(graph.filter(
                null,
                RDF.TYPE,
                RepositoryConfigSchema.REPOSITORY
        )).orElse(null);

        graph.add(repositoryNode, RepositoryConfigSchema.REPOSITORYID,
                  SimpleValueFactory.getInstance().createLiteral(repositoryId)
        );

        if (repositoryLabel != null) {
            graph.add(repositoryNode, RDFS.LABEL,
                      SimpleValueFactory.getInstance().createLiteral(repositoryLabel)
            );
        }

        if (overrides != null) {
            Resource configNode = (Resource) Models.object(graph.filter(
                    null,
                    SailRepositorySchema.SAILIMPL,
                    null
            )).orElse(null);
            for (Map.Entry<String, String> e : overrides.entrySet()) {
                IRI key = SimpleValueFactory.getInstance().createIRI(OWLIMSailSchema.NAMESPACE + e.getKey());
                Literal value = SimpleValueFactory.getInstance().createLiteral(e.getValue());
                graph.remove(configNode, key, null);
                graph.add(configNode, key, value);
            }
        }

        RepositoryConfig repositoryConfig = RepositoryConfig.create(graph, repositoryNode);

        repositoryManager.addRepositoryConfig(repositoryConfig);
        repositoryManager.getAllRepositoryInfos(false).forEach(info -> {
            logger.info("repository info: {}", info.getId());
        });
        repository = (SailRepository) repositoryManager.getRepository(repositoryId);
    }

    public SailRepository getRepository() {
        return repository;
    }

    @Override
    public void close() throws IOException {
        var basedir = repositoryManager.getBaseDir();
        repositoryManager.shutDown();
        FileUtils.deleteDirectory(basedir);
    }

    public static EmbeddedGraphDB createTempRepository(InputStream repoConfiguration, String ruleset)
            throws IOException, RepositoryException,
            RDFParseException, RepositoryConfigException, RDFHandlerException {
        File baseDir = Files.createTempDirectory("graphdb-examples").toFile();

        final EmbeddedGraphDB embeddedGraphDB = new EmbeddedGraphDB(baseDir, repoConfiguration);

        if (ruleset == null) {
            ruleset = "empty";
        }
        embeddedGraphDB.createRepository("graphdb-repo", null, Map.of("ruleset", ruleset));

        return embeddedGraphDB;
    }
}

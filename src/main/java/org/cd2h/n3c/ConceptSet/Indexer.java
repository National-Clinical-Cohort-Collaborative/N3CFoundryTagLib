package org.cd2h.n3c.ConceptSet;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.cd2h.n3c.Foundry.util.LocalProperties;
import org.cd2h.n3c.Foundry.util.PropertyLoader;

public class Indexer {
	static String pathPrefix = "/usr/local/CD2H/lucene/";
    static Connection conn = null;

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		conn = getConnection("lucene");

		Directory indexDir = FSDirectory.open(new File(pathPrefix + "concept_sets"));
		Directory taxoDir = FSDirectory.open(new File(pathPrefix + "concept_sets_tax"));

		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43,	new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_43));
		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		IndexWriter indexWriter = new IndexWriter(indexDir, config);

		// Writes facet ords to a separate directory from the main index
		DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

		// Reused across documents, to add the necessary facet fields
		FacetFields facetFields = new FacetFields(taxoWriter);

		indexConceptSets(indexWriter, facetFields);

		taxoWriter.close();
		indexWriter.close();
	}
	
	static void indexConceptSets(IndexWriter indexWriter, FacetFields facetFields) {
		
	}

	public static Connection getConnection(String property_file) throws SQLException, ClassNotFoundException {
		LocalProperties prop_file = PropertyLoader.loadProperties(property_file);
		Class.forName("org.postgresql.Driver");
		Properties props = new Properties();
		props.setProperty("user", prop_file.getProperty("jdbc.user"));
		props.setProperty("password", prop_file.getProperty("jdbc.password"));
		Connection conn = DriverManager.getConnection(prop_file.getProperty("jdbc.url"), props);
		conn.setAutoCommit(false);
		return conn;
	}
}

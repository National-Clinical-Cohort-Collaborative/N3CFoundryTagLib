package org.cd2h.n3c.ConceptSet;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.cd2h.n3c.Foundry.util.LocalProperties;
import org.cd2h.n3c.Foundry.util.PropertyLoader;

import edu.uiowa.lucene.biomedical.BiomedicalAnalyzer;

public class Indexer {
	static Logger logger = Logger.getLogger(Indexer.class);
	static String pathPrefix = "/usr/local/CD2H/lucene/";
	static LocalProperties prop_file = null;
    static Connection conn = null;

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("concept_sets");
		conn = getConnection();

		Directory indexDir = FSDirectory.open(new File(pathPrefix + "concept_sets"));
		Directory taxoDir = FSDirectory.open(new File(pathPrefix + "concept_sets_tax"));

		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43,	new BiomedicalAnalyzer());
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
	
	@SuppressWarnings("deprecation")
	static void indexConceptSets(IndexWriter indexWriter, FacetFields facetFields) throws SQLException, IOException {
		int count = 0;
		PreparedStatement stmt = conn.prepareStatement("select codeset_id,concept_set_name,status from enclave_concept.code_sets where is_most_recent_version and status is not null");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			count++;
			
			int id = rs.getInt(1);
			String name = rs.getString(2);
			String status = rs.getString(3);
			logger.info("id: " + id + "\tstatus: " + status + "\tname: " + name);
			
		    Document theDocument = new Document();
		    List<CategoryPath> paths = new ArrayList<CategoryPath>();
		    
		    theDocument.add(new Field("id", id+"", Field.Store.YES, Field.Index.NOT_ANALYZED));
			theDocument.add(new Field("content", id+" ", Field.Store.NO, Field.Index.ANALYZED));
		    paths.add(new CategoryPath("N3C Status/"+status, '/'));
			theDocument.add(new Field("label", name, Field.Store.YES, Field.Index.NOT_ANALYZED));
			theDocument.add(new Field("content", name+" ", Field.Store.NO, Field.Index.ANALYZED));

		    PreparedStatement substmt = conn.prepareStatement("select domain_id,concept_code,concept_name,concept_class_id from enclave_concept.code_set_concept where not is_excluded and codeset_id = ?");
			substmt.setInt(1, id);
			ResultSet subrs = substmt.executeQuery();
			while (subrs.next()) {
				String domain_id = subrs.getString(1);
				String concept_code = subrs.getString(2);
				String concept_name = subrs.getString(3);
				String concept_class = subrs.getString(4);
				logger.info("\tdomain id: " + domain_id + "\tconcept code: " + concept_code + "\tname: " + concept_name + "\tclass: " + concept_class);

				paths.add(new CategoryPath("OMOP Domain/"+domain_id, '/'));
				theDocument.add(new Field("content", domain_id+" ", Field.Store.NO, Field.Index.ANALYZED));

				theDocument.add(new Field("content", concept_code+" ", Field.Store.NO, Field.Index.ANALYZED));
				theDocument.add(new Field("content", concept_name+" ", Field.Store.NO, Field.Index.ANALYZED));

				paths.add(new CategoryPath("OMOP Class/"+concept_class, '/'));
				theDocument.add(new Field("content", concept_class+" ", Field.Store.NO, Field.Index.ANALYZED));
			}
			substmt.close();
			
		    facetFields.addFields(theDocument, paths);
		    indexWriter.addDocument(theDocument);
		}
		stmt.close();
		
		logger.info("concepts indexed: " + count);
	}

	public static Connection getConnection() throws SQLException, ClassNotFoundException {
		Class.forName("org.postgresql.Driver");
		Properties props = new Properties();
		props.setProperty("user", prop_file.getProperty("jdbc.user"));
		props.setProperty("password", prop_file.getProperty("jdbc.password"));
		Connection conn = DriverManager.getConnection(prop_file.getProperty("jdbc.url"), props);
		conn.setAutoCommit(false);
		return conn;
	}
}

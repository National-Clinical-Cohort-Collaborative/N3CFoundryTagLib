package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.PropertyLoader;

public class AchillesDataFetch extends CohortDataFetch {

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("enclave_data");
		initializeReserveHash();
		
		logger.info("fetching achilles_results_dist...");
		List<?> contents = APIRequest.fetchViewCSVFile("ri.foundry.main.dataset.e3b24de0-bbcf-4bd8-ad85-0be114fd6f68");
		logger.info("\tanalyzing...");
		attributes = processLabels(contents);
		setTypes(attributes, contents);
		logger.info("\tstoring...");
		storeData(generateSQLName("achilles_results_dist"), attributes, contents);

		logger.info("fetching achilles_results...");
		contents = APIRequest.fetchViewCSVFile("ri.foundry.main.dataset.416ba052-cd46-41e1-aa2e-9b709c45a938");
		logger.info("\tanalyzing...");
		attributes = processLabels(contents);
		setTypes(attributes, contents);
		logger.info("\tstoring...");
		storeData(generateSQLName("achilles_results"), attributes, contents);

		conn.close();
	}
}

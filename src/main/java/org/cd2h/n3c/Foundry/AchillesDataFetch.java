package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONObject;

public class AchillesDataFetch extends CohortDataFetch {

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("enclave_data");
		initializeReserveHash();
		
		List<?> contents = APIRequest.fetchViewCSVFile("ri.foundry.main.dataset.e3b24de0-bbcf-4bd8-ad85-0be114fd6f68");
		attributes = processLabels(contents);
		setTypes(attributes, contents);
		storeData(generateSQLName("achilles_result_dist"), attributes, contents);

		contents = APIRequest.fetchViewCSVFile("ri.foundry.main.dataset.416ba052-cd46-41e1-aa2e-9b709c45a938");
		attributes = processLabels(contents);
		setTypes(attributes, contents);
		storeData(generateSQLName("achilles_results"), attributes, contents);

		conn.close();
	}
}

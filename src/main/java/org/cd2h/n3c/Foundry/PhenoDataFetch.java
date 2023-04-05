package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONObject;

public class PhenoDataFetch extends CohortDataFetch {

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("enclave_data");
		initializeReserveHash();
		
		JSONObject result = APIRequest.fetchDirectory(prop_file.getProperty("pheno.directory"));
		process(result);

		JSONObject result2 = APIRequest.fetchDirectory(prop_file.getProperty("pheno.directory2"));
		process(result2);

		JSONObject result3 = APIRequest.fetchDirectory(prop_file.getProperty("pheno.directory3"));
		process(result3);

		conn.commit();
		conn.close();
	}
	
	static void process(JSONObject result) throws IOException, SQLException {
		JSONArray  array  = result.getJSONArray("values");
		for (int  i = 0; i < array.length();  i++)  {
			JSONObject element  = array.getJSONObject(i);
			String name  = element.getString("name");
			logger.info("name: " +  name);
			if (name.equals("Copy Approved Downloads"))
				continue;
			String rid  = element.getString("rid");
			logger.info("\trid:  " +  rid);
			process(name, rid);
		}
	}
}

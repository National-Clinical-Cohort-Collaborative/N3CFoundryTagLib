package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONObject;

public class PedsDataFetch extends CohortDataFetch {

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("n3c_peds");
		initializeReserveHash();
		
//		JSONObject result = APIRequest.fetchDirectory(prop_file.getProperty("peds.dashboard"));
//		process(result);

		process("hosptialized peds by month clamped over 20", prop_file.getProperty("peds.table"));
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
//			String contents = APIRequest.fetchCompassFile(name, rid);
			if (name.equals("Simple ggplot ggsave"))
				APIRequest.fetchCompassFile("myexample.svg", rid, "/Users/eichmann/Documents/Components/workspace/n3c-pediatrics-dashboard/WebContent/downloads/");

		}
	}
}

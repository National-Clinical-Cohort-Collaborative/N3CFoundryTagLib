package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.Foundry.util.APIRequest;
import org.cd2h.n3c.Foundry.util.LocalProperties;
import org.cd2h.n3c.Foundry.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONObject;

public class EnclaveMetricsFetch {
    static Logger logger = Logger.getLogger(EnclaveMetricsFetch.class);
    static LocalProperties prop_file = null;

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
	PropertyConfigurator.configure(args[0]);
	prop_file = PropertyLoader.loadProperties("n3c_foundry");
	Connection conn = APIRequest.getConnection(prop_file);

	JSONObject results = APIRequest.submit("n3c-website-metrics");
	logger.trace("results:\n" + results.toString(3));
	
	APIRequest.simpleStmt("truncate n3c_admin.enclave_stats");
	
	JSONArray hits = results.getJSONArray("hits");
	logger.info("hits:\n" + hits.toString(3));
	for (int i = 0; i < hits.length(); i++) {
	    JSONObject hit = hits.getJSONObject(i).getJSONObject("object");
	    String title = hit.getString("title");
	    int count = hit.getJSONObject("properties").getInt("value");
	    logger.info("title: " + title + "\tcount: " + count);
	    PreparedStatement stmt = conn.prepareStatement("insert into n3c_admin.enclave_stats values(?,?)");
	    stmt.setString(1, title);
	    stmt.setInt(2, count);
	    stmt.execute();
	    stmt.close();
	}
	conn.close();
    }

}

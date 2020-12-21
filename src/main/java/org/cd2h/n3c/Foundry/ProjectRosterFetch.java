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
import org.json.JSONException;
import org.json.JSONObject;

public class ProjectRosterFetch {
    static Logger logger = Logger.getLogger(ProjectRosterFetch.class);
    static LocalProperties prop_file = null;

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
	PropertyConfigurator.configure(args[0]);
	prop_file = PropertyLoader.loadProperties("n3c_foundry");
	Connection conn = APIRequest.getConnection(prop_file);

	JSONObject results = APIRequest.fetchJSONObject(prop_file, "n3c-website-approved-projects");
	logger.trace("results:\n" + results.toString(3));

	APIRequest.simpleStmt("truncate n3c_admin.enclave_project");
	
	JSONArray hits = results.getJSONArray("hits");
	logger.debug("hits:\n" + hits.toString(3));
	for (int i = 0; i < hits.length(); i++) {
	    JSONObject hit = hits.getJSONObject(i).getJSONObject("object");
	    try {
		String title = hit.getString("title");
		String uid = hit.getJSONObject("primaryKey").getString("project_uid");
		String statement = hit.getJSONObject("properties").getString("nonconfidential_research_statement");
		String investigator = hit.getJSONObject("properties").getString("lead_investigator");
		String task_team = hit.getJSONObject("properties").getString("is_task_team_project");
		String accessing_institution = hit.getJSONObject("properties").getString("accessing_institution");
		logger.info("title: " + title + "\tlead investigator: " + investigator + "\ttask team: " + task_team);
		logger.info("\taccessing_institution: " + accessing_institution);
		PreparedStatement stmt = conn.prepareStatement("insert into n3c_admin.enclave_project values(?,?,?,?,?::boolean,?)");
		stmt.setString(1, uid);
		stmt.setString(2, title);
		stmt.setString(3, statement);
		stmt.setString(4, investigator);
		stmt.setString(5, task_team);
		stmt.setString(6, accessing_institution);
		stmt.execute();
		stmt.close();
	    } catch (JSONException e) {
		logger.error("Error encountered: " + e);
		logger.error("hit: " + hit.toString(3));
	    }
	}
	conn.close();
	logger.info("total: " + hits.length());
    }
}

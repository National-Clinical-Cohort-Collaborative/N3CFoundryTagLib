package org.cd2h.n3c.Jira;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.LocalProperties;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class ExternalDatasetRequestFetch {
    static Logger logger = Logger.getLogger(ExternalDatasetRequestFetch.class);
    static LocalProperties prop_file = null;

    public static void main(String[] args) throws UnirestException, ClassNotFoundException, SQLException {
	PropertyConfigurator.configure(args[0]);
	prop_file = PropertyLoader.loadProperties("n3c_jira");
	Connection conn = APIRequest.getConnection(prop_file);

	APIRequest.simpleStmt("truncate n3c_admin.raw_jira");
	APIRequest.simpleStmt("truncate n3c_admin.enclave_external_dataset");

	// This code sample uses the 'Unirest' library:
	// http://unirest.io/java.html
	HttpResponse<JsonNode> response = Unirest
		.get("https://n3c-help.atlassian.net/rest/api/3/search?jql=project%20%3D%20%22EXTDATASET%22")
		.basicAuth("david-eichmann@uiowa.edu", prop_file.getProperty("api-token"))
		.header("Accept", "application/json")
		// .queryString("query", "jql=project%20%3D%20%22EXTDATASET%22")
		.asJson();

	// logger.info(response.getBody());
	JSONObject results = new JSONObject(new JSONTokener(response.getRawBody()));
	logger.debug("results:\n" + results.toString(3));
	JSONArray theArray = results.getJSONArray("issues");
	for (int i = 0; i < theArray.length(); i++) {
	    JSONObject issue = theArray.getJSONObject(i);
	    JSONObject fields = issue.getJSONObject("fields");
	    String id = issue.getString("id");
	    String key = issue.getString("key");
	    String summary = fields.getString("summary");
	    String created = fields.getString("created");
	    String updated = fields.getString("updated");
	    String priority = fields.getJSONObject("priority").getString("name");
	    String name = null;
	    String description = null;
	    String author_name = null;
	    String author_orcid = null;
	    String author_url = null;
	    String author_affiliation = null;
	    String license = null;
	    String cost = null;
	    String url = null;
	    String identifier = null;
	    String contain_geo_codes = null;
	    String contain_phi = null;
	    String domain_team_relevance = null;
	    String justification = null;
	    String contact = null;
	    String documentation = null;
	    String keywords = null;
	    String citation_name = null;
	    String citation_doi = null;
	    String citation_url = null;

	    logger.info("id: " + id);
	    logger.info("\tkey: " + key);
	    logger.info("\tsummary: " + summary);
	    logger.info("\tcreated: " + created);
	    logger.info("\tupdated: " + updated);
	    logger.info("\tpriority: " + priority);
	    logger.debug("issue:\n" + issue.toString(3));

	    if (fields.optJSONObject("description") != null) {
		JSONArray list = fields.getJSONObject("description").getJSONArray("content");
		for (int j = 0; j < list.length(); j++) {
		    JSONObject entry = list.getJSONObject(j);
		    if (!entry.getString("type").equals("bulletList"))
			continue;
		    JSONArray bullets = entry.getJSONArray("content");
		    for (int k = 0; k < bullets.length(); k++) {
			JSONObject bullet = bullets.getJSONObject(k);
			logger.debug("bullet: " + bullet.toString(3));
			JSONArray bulletContent = bullet.getJSONArray("content").getJSONObject(0).getJSONArray("content");
			String label = bulletContent.getJSONObject(0).getString("text").trim();
			String value = bulletContent.getJSONObject(1).getString("text").trim();
			logger.debug("label: " + label + "\tvalue: " + value);
			switch (label) {
			case "name:":
			    name = value;
			    break;
			case "description:":
			    description = value;
			    break;
			case "author.name:":
			    author_name = value;
			    break;
			case "author.orcid:":
			    author_orcid = value;
			    break;
			case "author.url:":
			    author_url = value;
			    break;
			case "author.affiliation.name:":
			    author_affiliation = value;
			    break;
			case "license:":
			    license = value;
			    break;
			case "cost:":
			    cost = value;
			    break;
			case "url:":
			    url = value;
			    break;
			case "identifier:":
			    identifier = value;
			    break;
			case "contain_geo_codes:":
			    contain_geo_codes = value;
			    break;
			case "contain_phi:":
			    contain_phi = value;
			    break;
			case "domain_team_relevance:":
			    domain_team_relevance = domain_team_relevance == null ? value : domain_team_relevance + "; " + value;
			    break;
			case "justification:":
			    justification = value;
			    break;
			case "contact:":
			    contact = value;
			    break;
			case "documentation:":
			    documentation = value;
			    break;
			case "keywords:":
			    keywords = value;
			    break;
			case "citation.name:":
			    citation_name = value;
			    break;
			case "citation.doi:":
			    citation_doi = value;
			    break;
			case "citation.url:":
			    citation_url = value;
			    break;
			default:
			    logger.error(">>>>>>> unknown label: " + label + "\tvalue: " + value);
			}
		    }
		}
	    }

	    PreparedStatement rawStmt = conn.prepareStatement("insert into n3c_admin.raw_jira values (?,?,?::jsonb)");
	    rawStmt.setString(1, issue.getString("id"));
	    rawStmt.setString(2, issue.getString("key"));
	    rawStmt.setString(3, issue.getJSONObject("fields").toString());
	    rawStmt.executeUpdate();
	    rawStmt.close();

	    int count = 1;
	    PreparedStatement stmt = conn.prepareStatement("insert into n3c_admin.enclave_external_dataset values (?::int,?,?,?::timestamp,?::timestamp,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	    stmt.setString(count++, id);
	    stmt.setString(count++, key);
	    stmt.setString(count++, summary);
	    stmt.setString(count++, created);
	    stmt.setString(count++, updated);
	    stmt.setString(count++, priority);
	    stmt.setString(count++, name);
	    stmt.setString(count++, description);
	    stmt.setString(count++, author_name);
	    stmt.setString(count++, author_orcid);
	    stmt.setString(count++, author_url);
	    stmt.setString(count++, author_affiliation);
	    stmt.setString(count++, license);
	    stmt.setString(count++, cost);
	    stmt.setString(count++, url);
	    stmt.setString(count++, identifier);
	    stmt.setString(count++, contain_geo_codes);
	    stmt.setString(count++, contain_phi);
	    stmt.setString(count++, domain_team_relevance);
	    stmt.setString(count++, justification);
	    stmt.setString(count++, contact);
	    stmt.setString(count++, documentation);
	    stmt.setString(count++, keywords);
	    stmt.setString(count++, citation_name);
	    stmt.setString(count++, citation_doi);
	    stmt.setString(count++, citation_url);
	    stmt.executeUpdate();
	    stmt.close();
	}
    }

}

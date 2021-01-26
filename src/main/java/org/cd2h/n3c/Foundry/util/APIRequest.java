package org.cd2h.n3c.Foundry.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONTokener;

import au.com.bytecode.opencsv.CSVReader;

public class APIRequest {
    static Logger logger = Logger.getLogger(APIRequest.class);
    static LocalProperties prop_file = null;
    protected static Character separator = ',';

	// curl -X POST "https://unite.nih.gov/phonograph2/api/objects/search/objects?pageSize=10000"
	//	-H "Authorization: Bearer $TOKEN"
	//	-H 'Content-Type: application/json'
	//	-d '{"filter":{"type":"matchAll","matchAll":{}},"aggregations":{},"objectTypes":["n3c-website-approved-projects"] }'

    public static JSONObject fetchJSONObject(LocalProperties pfile, String request) throws IOException {
	prop_file = pfile;
	return fetchJSONObject(request);
    }

    public static JSONObject fetchJSONObject(String request) throws IOException {
	// configure the connection
	URL uri = new URL(prop_file.getProperty("api.url"));
	logger.debug("url: " + uri);
	logger.debug("token: " + prop_file.getProperty("api.token"));
	HttpURLConnection con = (HttpURLConnection) uri.openConnection();
	con.setRequestMethod("POST"); // type: POST, PUT, DELETE, GET
	if (prop_file.getProperty("api.token") != null)
	    con.setRequestProperty("Authorization", "Bearer " + prop_file.getProperty("api.token"));
	con.setRequestProperty("Content-Type","application/json");
	con.setDoOutput(request != null);
	con.setDoInput(true);
	
	if (request != null) {
	    // submit the construct
	    logger.debug("request: " + request);
	    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(con.getOutputStream()));
	    out.write("{\"filter\":{\"type\":\"matchAll\",\"matchAll\":{}},\"aggregations\":{},\"objectTypes\":[\"" + request + "\"] }");
	    out.flush();
	    out.close();
	}

	// pull down the response JSON
	con.connect();
	logger.debug("response:" + con.getResponseCode());
	if (con.getResponseCode() >= 400) {
	    BufferedReader in = new BufferedReader(new InputStreamReader(con.getErrorStream()));
	    JSONObject results = new JSONObject(new JSONTokener(in));
	    logger.error("error:\n" + results.toString(3));
	    in.close();
	    return null;
	} else {
	    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
	    JSONObject results = new JSONObject(new JSONTokener(in));
	    logger.debug("results:\n" + results.toString(3));
	    in.close();
	    return results;
	}
    }

    // Compass documentation: https://unite.nih.gov/workspace/documentation/developer/api/compass/services/CompassService/endpoints/getChildren
    
    // curl -X GET \
    // -H "Authorization: Bearer $TOKEN" \
    // "https://unite.nih.gov/compass/api/folders/ri.compass.main.folder.6d86aeb7-dcbe-468b-b9dd-3c8299d45e5b/children"
    
    // curl -X GET \
    // -H "Authorization: Bearer $TOKEN" \
    // "https://unite.nih.gov/foundry-data-proxy/api/dataproxy/datasets/<datasetRid>/branches/master/csv"
    
    public static List<?> fetchCSVFile(LocalProperties pfile, String datasetRid) throws IOException {
	prop_file = pfile;
	return fetchCSVFile(datasetRid);
    }

    public static List<?> fetchCSVFile(String datasetRid) throws IOException {
	// configure the connection
	URL uri = new URL("https://unite.nih.gov/foundry-data-proxy/api/dataproxy/datasets/" + datasetRid + "/branches/master/csv?includeColumnNames=true");
	logger.debug("url: " + uri);
	logger.debug("token: " + prop_file.getProperty("api.token"));
	HttpURLConnection con = (HttpURLConnection) uri.openConnection();
	con.setRequestMethod("GET"); // type: POST, PUT, DELETE, GET
	if (prop_file.getProperty("api.token") != null)
	    con.setRequestProperty("Authorization", "Bearer " + prop_file.getProperty("api.token"));
//	con.setRequestProperty("Content-Type","application/json");
	con.setDoOutput(datasetRid != null);
//	con.setDoInput(true);
	
//	if (datasetRid != null) {
//	    // submit the construct
//	    logger.debug("datasetRid: " + datasetRid);
//	    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(con.getOutputStream()));
//	    out.write("{\"filter\":{\"type\":\"matchAll\",\"matchAll\":{}},\"aggregations\":{},\"objectTypes\":[\"" + datasetRid + "\"] }");
//	    out.flush();
//	    out.close();
//	}

	// pull down the response CSV
	con.connect();
	logger.debug("response:" + con.getResponseCode());
	if (con.getResponseCode() >= 400) {
	    BufferedReader in = new BufferedReader(new InputStreamReader(con.getErrorStream()));
	    JSONObject results = new JSONObject(new JSONTokener(in));
	    logger.error("error:\n" + results.toString(3));
	    in.close();
	    return null;
	} else {
	    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		CSVReader csvr = new CSVReader(in, separator);
		List<?> contents = csvr.readAll();
	    logger.debug("contents:\n" + contents);
	    in.close();
	    return contents;
	}
    }

    public static Connection getConnection(LocalProperties pfile) throws ClassNotFoundException, SQLException {
	prop_file = pfile;
	return getConnection();
    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
	logger.debug("connecting to database...");
	Class.forName("org.postgresql.Driver");
	Properties props = new Properties();
	props.setProperty("user", prop_file.getProperty("jdbc.user"));
	props.setProperty("password", prop_file.getProperty("jdbc.password"));
	Connection conn = DriverManager.getConnection(prop_file.getProperty("jdbc.url"), props);
	conn.setAutoCommit(true);
	return conn;
    }

    public static void simpleStmt(String queryString) {
	try {
	    logger.info("executing " + queryString + "...");
	    PreparedStatement beginStmt = getConnection().prepareStatement(queryString);
	    beginStmt.executeUpdate();
	    beginStmt.close();
	} catch (Exception e) {
	    logger.error("Error in database initialization: ", e);
	}
    }
}

package org.cd2h.n3c.ConceptSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.LocalProperties;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

public class Publisher {
	static Logger logger = Logger.getLogger(Publisher.class);
	static String pathPrefix = "/usr/local/CD2H/lucene/";
	static LocalProperties prop_file = null;
	static LocalProperties zenodo_props = null;
    static Connection conn = null;
    
    static String siteName = null;
    static String token = null;

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, InterruptedException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("concept_sets");
		pathPrefix = prop_file.getProperty("md_path");
		conn = getConnection();
		
		zenodo_props = PropertyLoader.loadProperties("zenodo.properties");
		siteName = zenodo_props.getProperty("site");
		if (siteName.startsWith("sandbox"))
			token = zenodo_props.getProperty("sandbox_token");
		else
			token = zenodo_props.getProperty("access_token");
		
//		reserve();
		deposit();
	}
	
	static void reserve() throws SQLException, IOException {
		PreparedStatement stmt = conn.prepareStatement("select distinct codeset_id, alias from enclave_concept.concept_set where provisional_approval_date is not null and not exists (select codeset_id from enclave_concept.zenodo_deposit_raw where concept_set.codeset_id=zenodo_deposit_raw.codeset_id)");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			String alias = rs.getString(2);
			logger.info("depositing: " + id + " : " + alias);
			
			JSONObject creation = newDeposition(id, alias);
			PreparedStatement depstmt = conn.prepareStatement("insert into enclave_concept.zenodo_deposit_raw(codeset_id,raw) values(?,?::jsonb)");
			depstmt.setInt(1, id);
			depstmt.setString(2, creation.toString(3));
			depstmt.execute();
			depstmt.close();
		}
		stmt.close();
		conn.commit();
	}
	
	static void deposit() throws IOException, JSONException, SQLException, InterruptedException {
		int count = 0;
		PreparedStatement stmt = conn.prepareStatement("select distinct concept_set.codeset_id, bucket from enclave_concept.concept_set, enclave_concept.zenodo_deposit "
														+ "where provisional_approval_date is not null "
														+ "and concept_set.codeset_id = zenodo_deposit.codeset_id "
														+ "and not exists (select codeset_id from enclave_concept.zenodo_file_raw "
														+ "					where concept_set.codeset_id=zenodo_file_raw.codeset_id)");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			String bucket = rs.getString(2);
			JSONObject file = addFile(bucket, id, "pdf");
			PreparedStatement filestmt = conn.prepareStatement("insert into enclave_concept.zenodo_file_raw values(?, ?::jsonb, 'pdf')");
			filestmt.setInt(1, id);
			filestmt.setString(2, file.toString(3));
			filestmt.execute();
			filestmt.close();
			
			if ((new File("concepts/" + id + ".json")).exists()) {
				JSONObject jsonFile = addFile(bucket, id, "json");
				PreparedStatement jsonFilestmt = conn.prepareStatement("insert into enclave_concept.zenodo_file_raw values(?, ?::jsonb, 'json')");
				jsonFilestmt.setInt(1, id);
				jsonFilestmt.setString(2, jsonFile.toString(3));
				jsonFilestmt.execute();
				jsonFilestmt.close();
			}
			
			if (++count % 90 == 0)
				Thread.sleep(30*1000);
		}
		stmt.close();

		JSONArray result = fetchDepositions();
		logger.info("depositions: " + result.toString(3));
		
		conn.commit();
	}
	
	static JSONArray fetchDepositions() throws IOException {
		// configure the connection
		URL uri = new URL("https://" + siteName + "/api/deposit/depositions");
		logger.info("url: " + uri);
		HttpURLConnection con = (HttpURLConnection) uri.openConnection();
		con.setRequestMethod("GET"); // type: POST, PUT, DELETE, GET
		con.setRequestProperty("Authorization", "Bearer " + token);
		con.setRequestProperty("Content-Type", "application/json");
		con.setDoOutput(true);
		con.setDoInput(true);

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
			JSONArray results = new JSONArray(new JSONTokener(in));
			logger.debug("results:\n" + results.toString(3));
			in.close();
			return results;
		}
	}
		
	static JSONObject newDeposition(int id, String alias) throws IOException {
		// configure the connection
		URL uri = new URL("https://" + siteName + "/api/deposit/depositions");
		logger.info("url: " + uri);
		HttpURLConnection con = (HttpURLConnection) uri.openConnection();
		con.setRequestMethod("POST"); // type: POST, PUT, DELETE, GET
		con.setRequestProperty("Authorization", "Bearer " + token);
		con.setRequestProperty("Content-Type", "application/json");
		con.setDoOutput(true);
		con.setDoInput(true);
		
		JSONObject metadata = new JSONObject();
		metadata.accumulate("title", "N3C Concept Set - " + id +" (" + alias + ")");
		metadata.accumulate("description", "A list of concepts from the standardized vocabulary that taken together describe a topic of interest for a study.");
		metadata.accumulate("upload_type", "publication");
		metadata.accumulate("publication_type", "workingpaper");
		JSONObject payload = new JSONObject();
		payload.put("metadata", metadata);
		logger.info("payload: " + payload.toString(3));
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(con.getOutputStream()));
		out.write(payload.toString());
		out.flush();
		out.close();


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
	
	static JSONObject addFile(String prefix, int concept_set_id, String suffix) throws IOException {
		// configure the connection
		URL uri = new URL(prefix + "/" + concept_set_id + "." + suffix);
		logger.info("url: " + uri);
		HttpURLConnection con = (HttpURLConnection) uri.openConnection();
		con.setRequestMethod("PUT"); // type: POST, PUT, DELETE, GET
		con.setRequestProperty("Authorization", "Bearer " + token);
		con.setRequestProperty("Content-Type", "application/octet-stream");
		con.setDoOutput(true);
		con.setDoInput(true);

		OutputStream outputStream = con.getOutputStream();
        FileInputStream inputStream = new FileInputStream(new File("concepts/" + concept_set_id + "." + suffix));
        byte[] buffer = new byte[4096];
        int bytesRead = -1;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }
        outputStream.flush();
        outputStream.close();
        inputStream.close();

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

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

import javax.servlet.jsp.JspTagException;

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
		
		if ("reserve".equals(args[1]))
			reserve();
		else if ("deposit".equals(args[1]))
			deposit();
		else if ("publish".equals(args[1]))
			publish();
		else if ("list".equals(args[1]))
			list();
		else if ("version".equals(args[1]))
			version();
		else if ("version_full".equals(args[1]))
			version_full();
		else if ("version_deposit".equals(args[1]))
			version_deposit();
		else if ("version_publish".equals(args[1]))
			version_publish();
		else if ("version_list".equals(args[1]))
			version_list();
		else if ("deposition_list".equals(args[1])) {
			fetchDepositions();
			conn.commit();
		} else
			logger.info("\nAction must be one of: reserve, deposit, publish\n\n");
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
	
	static void publish() throws IOException, JSONException, SQLException, InterruptedException {
		int count = 0;
		PreparedStatement stmt = conn.prepareStatement("select distinct concept_set.codeset_id, publish from enclave_concept.concept_set, enclave_concept.zenodo_deposit "
														+ "where not exists (select codeset_id from enclave_concept.zenodo_published "
														+ "						where zenodo_published.codeset_id = zenodo_deposit.codeset_id) "
														+ "and concept_set.codeset_id = zenodo_deposit.codeset_id "
														+ "and exists (select codeset_id from enclave_concept.zenodo_file_raw "
														+ "					where concept_set.codeset_id=zenodo_file_raw.codeset_id)");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			String publish = rs.getString(2);
			logger.info("publishing " + id);
			JSONObject deposit = publishDeposition(publish);
			
			PreparedStatement updateStmt = conn.prepareStatement("insert into enclave_concept.zenodo_published_raw values(?, now(), ?::jsonb)");
			updateStmt.setInt(1, id);
			updateStmt.setString(2, deposit.toString(3));
			updateStmt.execute();
			updateStmt.close();
		}
		stmt.close();
		
		conn.commit();
	}
	
	static void version() throws SQLException, IOException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		stmt = conn.prepareStatement("select distinct codeset_id, alias,zenodo_id from enclave_concept.concept_set natural join enclave_concept.zenodo_deposit where codeset_id not in (select codeset_id from enclave_concept.zenodo_version_raw)");
		rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			String alias = rs.getString(2);
			int zid = rs.getInt(3);
			logger.info("versioning: " + id + " : " + alias + " : " + zid);
			
			JSONObject version = newVersion(id,zid);
			PreparedStatement depstmt = conn.prepareStatement("insert into enclave_concept.zenodo_version_raw(codeset_id,raw) values(?,?::jsonb)");
			depstmt.setInt(1, id);
			depstmt.setString(2, version.toString(3));
			depstmt.execute();
			depstmt.close();
			conn.commit();
		}
		stmt.close();
		
		stmt = conn.prepareStatement("select distinct codeset_id, zenodo_id,latest_draft from enclave_concept.zenodo_version where codeset_id not in (select codeset_id from enclave_concept.zenodo_version_deposition_raw)");
		rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			int zid = rs.getInt(2);
			String alias = rs.getString(3);
			String latest = rs.getString(4);
			logger.info("versioning draft: " + id + " : " + latest + " : " + zid);
			
			JSONObject version = versionDeposition(id, alias, latest);
			PreparedStatement depstmt = conn.prepareStatement("insert into enclave_concept.zenodo_version_deposition_raw(codeset_id,raw) values(?,?::jsonb)");
			depstmt.setInt(1, id);
			depstmt.setString(2, version.toString(3));
			depstmt.execute();
			depstmt.close();
			conn.commit();
		}
		stmt.close();
	}
	
	static void version_full() throws SQLException, IOException {
		execute("truncate enclave_concept.zenodo_version_raw");
		execute("truncate enclave_concept.zenodo_version_deposition_raw");
		execute("truncate enclave_concept.zenodo_version_file_raw");
		execute("truncate enclave_concept.zenodo_version_published_raw");
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		stmt = conn.prepareStatement("select codeset_id, substring(doi from '[0-9]+$')::int as zenodo_id from enclave_concept.zenodo_master where doi is not null");
		rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			int zid = rs.getInt(2);
			logger.info("versioning: " + id + " : " + zid);
			
			JSONObject version = newVersion(id,zid);
			PreparedStatement depstmt = conn.prepareStatement("insert into enclave_concept.zenodo_version_raw(codeset_id,raw) values(?,?::jsonb)");
			depstmt.setInt(1, id);
			depstmt.setString(2, version.toString(3));
			depstmt.execute();
			depstmt.close();
			conn.commit();
		}
		stmt.close();
		
		stmt = conn.prepareStatement("select distinct codeset_id, zenodo_id, alias, latest_draft from enclave_concept.zenodo_version natural join enclave_concept.concept_set");
		rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			int zid = rs.getInt(2);
			String alias = rs.getString(3);
			String latest = rs.getString(4);
			logger.info("versioning draft: " + id + " : " + latest + " : " + zid);
			
			JSONObject version = versionDeposition(id, alias, latest);
			PreparedStatement depstmt = conn.prepareStatement("insert into enclave_concept.zenodo_version_deposition_raw(codeset_id,raw) values(?,?::jsonb)");
			depstmt.setInt(1, id);
			depstmt.setString(2, version.toString(3));
			depstmt.execute();
			depstmt.close();
			conn.commit();
		}
		stmt.close();
	}
	
	static void version_deposit() throws IOException, JSONException, SQLException, InterruptedException {
		int count = 0;
		PreparedStatement stmt = conn.prepareStatement("select codeset_id,zenodo_id,bucket from enclave_concept.zenodo_version_deposition where codeset_id not in (select codeset_id from enclave_concept.zenodo_version_file_raw)");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			int zid = rs.getInt(2);
			String bucket = rs.getString(3);
			JSONObject file = addFile(bucket, id, "pdf");
			PreparedStatement filestmt = conn.prepareStatement("insert into enclave_concept.zenodo_version_file_raw values(?, ?::jsonb, 'pdf')");
			filestmt.setInt(1, id);
			filestmt.setString(2, file.toString(3));
			filestmt.execute();
			filestmt.close();
			
			if ((new File("concepts/" + id + ".json")).exists()) {
				JSONObject jsonFile = addFile(bucket, id, "json");
				PreparedStatement jsonFilestmt = conn.prepareStatement("insert into enclave_concept.zenodo_version_file_raw values(?, ?::jsonb, 'json')");
				jsonFilestmt.setInt(1, id);
				jsonFilestmt.setString(2, jsonFile.toString(3));
				jsonFilestmt.execute();
				jsonFilestmt.close();
			}
			
			if (++count % 45 == 0)
				Thread.sleep(30*1000);

			conn.commit();
		}
		stmt.close();

//		JSONArray result = fetchDepositions();
//		logger.info("depositions: " + result.toString(3));
		
	}
	
	static void version_publish() throws SQLException, IOException {
		PreparedStatement stmt = conn.prepareStatement("select codeset_id,zenodo_id,publish from enclave_concept.zenodo_version_deposition where codeset_id not in (select codeset_id from enclave_concept.zenodo_version_published)");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			int zid = rs.getInt(2);
			String publish = rs.getString(3);
			logger.info("publish: " + id + " : " + zid + " : " + publish);
			JSONObject deposit = publishDeposition(publish);
			
			PreparedStatement updateStmt = conn.prepareStatement("insert into enclave_concept.zenodo_version_published_raw values(?, now(), ?::jsonb)");
			updateStmt.setInt(1, id);
			updateStmt.setString(2, deposit.toString(3));
			updateStmt.execute();
			updateStmt.close();
		}
		stmt.close();
		
		conn.commit();
	}
	
	static void version_list() throws SQLException, IOException {
//		PreparedStatement stmt = conn.prepareStatement("select distinct codeset_id, zenodo_id,latest_draft from enclave_concept.zenodo_version");
//		ResultSet rs = stmt.executeQuery();
//		while (rs.next()) {
//			int id = rs.getInt(1);
//			int zid = rs.getInt(2);
//			String latest = rs.getString(3);
//			logger.info("retrieving: " + id + " : " + latest + " : " + zid);
//			
//			JSONObject version = versionDeposition(latest);
//			PreparedStatement depstmt = conn.prepareStatement("insert into enclave_concept.zenodo_published_raw(codeset_id,raw) values(?,?::jsonb)");
//			depstmt.setInt(1, id);
//			depstmt.setString(2, version.toString(3));
//			depstmt.execute();
//			depstmt.close();
//			conn.commit();
//		}
//		stmt.close();
	}
	
	static void list() throws SQLException, IOException {
		PreparedStatement stmt = conn.prepareStatement("select distinct codeset_id, zenodo_id,latest_draft from enclave_concept.zenodo_version");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			int zid = rs.getInt(2);
			String latest = rs.getString(3);
			logger.info("retrieving: " + id + " : " + latest + " : " + zid);

			URL uri = new URL("https://" + siteName + "/api/records/" + zid);
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
			} else {
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				JSONObject results = new JSONObject(new JSONTokener(in));
				logger.info("id: " + id + "\tzid: " + zid + "\tresults:\n" + results.toString(3));
				in.close();

				PreparedStatement depstmt = conn.prepareStatement("insert into enclave_concept.zenodo_published_raw(codeset_id,raw) values(?,?::jsonb)");
				depstmt.setInt(1, id);
				depstmt.setString(2, results.toString(3));
				depstmt.execute();
				depstmt.close();
				conn.commit();
			}
		}
	}
	
	static JSONObject publishDeposition(String publishURI) throws IOException {
		// configure the connection
		URL uri = new URL(publishURI);
		HttpURLConnection con = (HttpURLConnection) uri.openConnection();
		con.setRequestMethod("POST"); // type: POST, PUT, DELETE, GET
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
			logger.error("error: " + con.getResponseCode());
			logger.error("results:\n" + results.toString(3));
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
		
	// TODO - paginate this
	
	static JSONArray fetchDepositions() throws IOException, SQLException {
		// configure the connection
		URL uri = new URL("https://" + siteName + "/api/deposit/depositions?size=200");
		logger.info("url: " + uri);
		HttpURLConnection con = (HttpURLConnection) uri.openConnection();
		con.setRequestMethod("GET"); // type: POST, PUT, DELETE, GET
		con.setRequestProperty("Authorization", "Bearer " + token);
		con.setRequestProperty("Content-Type", "application/json");
		con.setDoOutput(true);
		con.setDoInput(true);
		
		execute("truncate enclave_concept.zenodo_deposition_full_raw");

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
			in.close();
			for (int i = 0; i < results.length(); i++) {
				if (results.isNull(i))
					continue;
				JSONObject theObject = results.getJSONObject(i);
				logger.info("object: " + theObject.toString(3));
				PreparedStatement depstmt = conn.prepareStatement("insert into enclave_concept.zenodo_deposition_full_raw(raw) values(?::jsonb)");
				depstmt.setString(1, theObject.toString(3));
				depstmt.execute();
				depstmt.close();
			}
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
		metadata.accumulate("prereserve_doi", "Yes");

		JSONObject creator = new JSONObject();
		creator.accumulate("name", "Applicable Data Methods & Standards Domain Team");
		creator.accumulate("affiliation", "N3C");
		JSONArray creators = new JSONArray();
		creators.put(creator);
		metadata.put("creators", creators);
		
		JSONObject community = new JSONObject();
		community.accumulate("identifier", "cd2h-covid");
		JSONArray communities = new JSONArray();
		communities.put(community);
		metadata.put("communities", communities);
		
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
	
	static void versionClear(int id, int zid) throws IOException {
		URL uri = new URL("https://" + siteName + "/api/deposit/depositions/" + zid + "/files");
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
		} else {
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			JSONArray results = new JSONArray(new JSONTokener(in));
			logger.info("id: " + id + "\tzid: " + zid + "\tresults:\n" + results.toString(3));
			in.close();
			for (int i = 0; i < results.length(); i++) {
				if (results.isNull(i))
					continue;
				JSONObject theObject = results.getJSONObject(i);
				logger.info("object: " + theObject.toString(3));
			}
		}
	}
		
	static JSONObject newVersion(int id, int zid) throws IOException {
		// check for already existing files in this version and remove them
		versionClear(id, zid);
		
		// configure the connection
		URL uri = new URL("https://" + siteName + "/api/deposit/depositions/" + zid + "/actions/newversion");
		logger.info("url: " + uri);
		HttpURLConnection con = (HttpURLConnection) uri.openConnection();
		con.setRequestMethod("POST"); // type: POST, PUT, DELETE, GET
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
			JSONObject results = new JSONObject(new JSONTokener(in));
			logger.debug("results:\n" + results.toString(3));
			in.close();
			return results;
		}
	}
	
	static JSONObject versionDeposition(int id, String alias, String url) throws IOException {
		// configure the connection
		URL uri = new URL(url);
		logger.info("url: " + uri);
		HttpURLConnection con = (HttpURLConnection) uri.openConnection();
		con.setRequestMethod("PUT"); // type: POST, PUT, DELETE, GET
		con.setRequestProperty("Authorization", "Bearer " + token);
		con.setRequestProperty("Content-Type", "application/json");
		con.setDoOutput(true);
		con.setDoInput(true);
		
		JSONObject metadata = new JSONObject();
		metadata.accumulate("publication_date", "2024-01-30");
		metadata.accumulate("title", "N3C Concept Set - " + id +" (" + alias + ")");
		metadata.accumulate("description", "A list of concepts from the standardized vocabulary that taken together describe a topic of interest for a study.");
		metadata.accumulate("upload_type", "publication");
		metadata.accumulate("publication_type", "workingpaper");

		JSONObject creator = new JSONObject();
		creator.accumulate("name", "Applicable Data Methods & Standards Domain Team");
		creator.accumulate("affiliation", "N3C");
		JSONArray creators = new JSONArray();
		creators.put(creator);
		metadata.put("creators", creators);
		
		JSONObject community = new JSONObject();
		community.accumulate("identifier", "cd2h-covid");
		JSONArray communities = new JSONArray();
		communities.put(community);
		metadata.put("communities", communities);
		
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
	
	public static Connection getConnection() throws SQLException, ClassNotFoundException {
		Class.forName("org.postgresql.Driver");
		Properties props = new Properties();
		props.setProperty("user", prop_file.getProperty("jdbc.user"));
		props.setProperty("password", prop_file.getProperty("jdbc.password"));
		Connection conn = DriverManager.getConnection(prop_file.getProperty("jdbc.url"), props);
		conn.setAutoCommit(false);
		return conn;
	}
	
	static void execute(String statement) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(statement);
        stmt.executeUpdate();
        stmt.close();
	}

}

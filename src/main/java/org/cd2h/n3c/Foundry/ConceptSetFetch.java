package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.Foundry.CohortDataFetch.Attribute;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.LocalProperties;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ConceptSetFetch {
    static Logger logger = Logger.getLogger(ConceptSetFetch.class);
    static LocalProperties prop_file = null;
	static Attribute[] attributes = null;
	static Connection conn = null;
	static boolean load = true;

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("enclave_concept");

		JSONObject result = APIRequest.fetchDirectory(prop_file.getProperty("concept.directory"));
		JSONArray array = result.getJSONArray("values");
		for (int i = 0; i < array.length(); i++) {
			JSONObject element = array.getJSONObject(i);
			String name = element.getString("name");
			if (name.equals("Copy Approved Downloads"))
				continue;
			logger.info("name: " + name);
			String rid = element.getString("rid");
			logger.info("\trid:  " + rid);
			if (rid.equals(prop_file.getProperty("concept.member"))
					|| rid.equals(prop_file.getProperty("concept.version"))
					|| rid.equals(prop_file.getProperty("concept.provisional"))
					|| rid.equals(prop_file.getProperty("concept.project"))
					|| rid.equals(prop_file.getProperty("concept.container"))) {
				process(name,rid);
			}
		}
		
		PreparedStatement stmt = conn.prepareStatement("select codeset_id,atlas_json_resource_url from code_sets where atlas_json is null and atlas_json_resource_url is not null");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int id = rs.getInt(1);
			String resourceURL = rs.getString(2);
			try {
				JSONObject atlas = APIRequest.fetchCompassJSONObject("https://unite.nih.gov/blobster/api/salt/" + resourceURL + "/token");
				logger.info("id: " + id + "\turl" + resourceURL);
				logger.info(atlas.toString(3));
				
				PreparedStatement update = conn.prepareStatement("update code_sets set atlas_json = ? where codeset_id = ?");
				update.setString(1, atlas.toString(3));
				update.setInt(2, id);
				update.execute();
				update.close();
			} catch (JSONException e) {
				logger.error("error retrieving JSON for id");
			} catch (IOException e) {
				logger.error("error retrieving JSON for id");
			}
		}
		stmt.close();
	}

	static void process(String  enclaveTableName, String fileID) throws IOException, SQLException {
		String tableName = enclaveTableName;
		if (tableName.startsWith("[CC] ") || tableName.startsWith("[CC[ "))
			tableName = tableName.substring(5);
		if (tableName.startsWith("[CC Export] "))
			tableName = tableName.substring(12);
		logger.info("table name: " + tableName + "\tfile ID: " + fileID);
		List<?> contents = APIRequest.fetchCSVFile(prop_file, fileID);
		if (contents == null)
			return;
		attributes = CohortDataFetch.processLabels(contents);
		CohortDataFetch.setTypes(attributes, contents);
		storeData(CohortDataFetch.generateSQLName(tableName), attributes, contents);
	}

	@SuppressWarnings("deprecation")
	static void storeData(String tableName, Attribute[] attributes, List<?> contents) throws SQLException {
		StringBuffer createBuffer = new StringBuffer("create table if not exists " + tableName + "(");
		StringBuffer insertBuffer = new StringBuffer("insert into " + tableName + " values (");

		for (int i = 0; i < attributes.length; i++) {
			createBuffer.append((i == 0 ? "" : ", ") + attributes[i].label + " " + attributes[i].type);
			insertBuffer.append((i == 0 ? "" : ", ") + "?");
		}

		createBuffer.append(")");
		logger.debug("create command: " + createBuffer);
		if (load)
			simpleStmt(createBuffer.toString());		
		simpleStmt("truncate table " + tableName);

		insertBuffer.append(")");
		logger.debug("insert command: " + insertBuffer);

		PreparedStatement insertStmt = conn.prepareStatement(insertBuffer.toString());
		Iterator<?> iterator = contents.iterator();
		iterator.next();
		while (iterator.hasNext()) {
			String[] contentArray = (String[]) iterator.next();
			for (int i = 0; i < attributes.length; i++) {
				if (contentArray.length <= i || contentArray[i] == null || contentArray[i].trim().length() == 0)
					insertStmt.setNull(i + 1, CohortDataFetch.sqlNullType(attributes[i].type));
				else if (attributes[i].type.equals("int"))
					insertStmt.setInt(i + 1, Integer.parseInt(contentArray[i]));
				else if (attributes[i].type.equals("bigint"))
					insertStmt.setLong(i + 1, Long.parseLong(contentArray[i]));
				else if (attributes[i].type.equals("float"))
					insertStmt.setFloat(i + 1, Float.parseFloat(contentArray[i]));
				else if (attributes[i].type.equals("boolean"))
					insertStmt.setBoolean(i + 1, (contentArray[i].toLowerCase().startsWith("y")
							| contentArray[i].toLowerCase().startsWith("t")));
				else if (attributes[i].type.equals("date"))
					insertStmt.setDate(i + 1, new java.sql.Date((new java.util.Date(contentArray[i])).getTime()));
				else
					insertStmt.setString(i + 1, contentArray[i].replace(" / ", "\n").trim());
			}
			insertStmt.executeUpdate();
		}
		insertStmt.close();
	}

	public static void simpleStmt(String queryString) {
		try {
			logger.info("executing " + queryString + "...");
			PreparedStatement beginStmt = conn.prepareStatement(queryString);
			beginStmt.executeUpdate();
			beginStmt.close();
		} catch (Exception e) {
			logger.error("Error in database initialization: ", e);
		}
	}
}

package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
import org.json.JSONObject;

public class SiteStatusFetch {
    static Logger logger = Logger.getLogger(SiteStatusFetch.class);
    static LocalProperties prop_file = null;
	static Attribute[] attributes = null;
	static Connection conn = null;
	static boolean load = true;

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("n3c_maps");

		JSONObject result = APIRequest.fetchDirectory(prop_file.getProperty("site.status"));
		JSONArray array = result.getJSONArray("values");
		for (int i = 0; i < array.length(); i++) {
			JSONObject element = array.getJSONObject(i);
			String name = element.getString("name");
			if (name.equals("Copy Approved Downloads"))
				continue;
			logger.info("name: " + name);
			String rid = element.getString("rid");
			logger.info("\trid:  " + rid);
			process(name, rid);
			conn.commit();
		}
		conn.close();
	}

	static void process(String  enclaveTableName, String fileID) throws IOException, SQLException {
		String tableName = enclaveTableName;
		if (tableName.startsWith("[CC] ") || tableName.startsWith("[CC[ "))
			tableName = tableName.substring(5);
		if (tableName.startsWith("[CC Export] "))
			tableName = tableName.substring(12);
		logger.info("table name: " + tableName + "\tfile ID: " + fileID);
		List<?> contents = APIRequest.fetchCSVFile(prop_file, fileID);
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

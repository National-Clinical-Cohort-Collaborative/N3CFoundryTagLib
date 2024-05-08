package org.cd2h.n3c.Airtable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.Attribute;
import org.cd2h.n3c.util.DatabaseAnalyzer;
import org.cd2h.n3c.util.LocalProperties;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONObject;

public class DTADUAPull {
    static boolean load = true;
	static Logger logger = Logger.getLogger(DTADUAPull.class);
	static LocalProperties prop_file = null;
    static Connection conn = null;

	static Attribute[] attributes = null;
	static Hashtable<String, String> attributeHash = new Hashtable<String, String>();
	static Hashtable<String, String> reservedHash = new Hashtable<String, String>();
	
	static Hashtable<String, String> forceTextHash = new Hashtable<String, String>();
	
	static DatabaseAnalyzer analyzer = null;
	static String urlPrefix = "https://api.airtable.com/v0/";

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("airtable");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("airtable");
		initializeReserveHash();
		
		simpleStmt("create table if not exists table_list(raw jsonb)");
		simpleStmt("truncate table_list");
		
		String request = urlPrefix + "meta/bases/" + prop_file.getProperty("api.baseID") + "/tables";
		JSONObject result = APIRequest.fetchCompassJSONObject(request);
		JSONArray  array  = result.getJSONArray("tables");
		for (int  i = 0; i < array.length();  i++)  {
			JSONObject element  = array.getJSONObject(i);
			logger.info("table: " +  element.toString(3));
			PreparedStatement stmt = conn.prepareStatement("insert into table_list values(?::jsonb)");
			stmt.setString(1, element.toString());
			stmt.execute();
			stmt.close();
			conn.commit();
		}
		for (int  i = 0; i < array.length();  i++)  {
			JSONObject element  = array.getJSONObject(i);
			String id = element.getString("id");
			String name = element.getString("name");
			logger.info("id: " + id + " : " + name);
			process(name,id);
			conn.commit();
		}
		
		conn.close();
	}
	
	static void process(String airtableName, String fileID) throws IOException, SQLException {
		String tableName = generateSQLName(airtableName.toLowerCase()+"_raw");
		simpleStmt("create table if not exists " + tableName + "(raw jsonb)");
		simpleStmt("truncate " + tableName);

		String request = urlPrefix + prop_file.getProperty("api.baseID") + "/" + fileID;
		String offset = "";
				
		do {
			JSONObject result = APIRequest.fetchCompassJSONObject(request+(offset.equals("") ? "" : "?offset="+offset));
			JSONArray array = result.getJSONArray("records");
			for (int i = 0; i < array.length(); i++) {
				JSONObject element = array.getJSONObject(i);
				logger.info("record: " + element.getString("id"));
				PreparedStatement stmt = conn.prepareStatement("insert into " + tableName + " values(?::jsonb)");
				stmt.setString(1, element.toString());
				stmt.execute();
				stmt.close();
			}
			
			offset = result.optString("offset");
			logger.info("offset: " + offset);
		} while (!offset.equals(""));
	}

	static String generateSQLName(String attribute) {
		return generateSQLName(attribute,false);
	}

	static String generateSQLName(String attribute, boolean simpleName) {
		if (attribute == null || attribute.length() == 0)
			return "";
		String attributeBase = attribute.trim().replace("\uFEFF", "");
		attributeBase = (Character.isJavaIdentifierStart(attributeBase.charAt(0)) ? "" : "x___")
				+ attributeBase.replaceAll("[^A-Za-z0-9_]", "_");
		
		if (simpleName)
			return attributeBase.toLowerCase();
		
		if (reservedHash.containsKey(attributeBase.toUpperCase()))
			attributeBase = "x__" + attributeBase;
		if (attributeBase.length() > 60)
			attributeBase = attributeBase.substring(0, 60);
		if (!attributeHash.containsKey(attributeBase.toLowerCase())) {
			attributeHash.put(attributeBase.toLowerCase(), attributeBase);
			return attributeBase;
		}
		int count = 1;
		while (true) {
			String newAttribute = attributeBase + "_" + count;
			if (!attributeHash.containsKey(newAttribute.toLowerCase())) {
				attributeHash.put(newAttribute.toLowerCase(), newAttribute);
				return newAttribute;
			}
			count++;
		}
	}

	static void simpleStmt(String queryString) {
		try {
			logger.info("executing " + queryString + "...");
			PreparedStatement beginStmt = conn.prepareStatement(queryString);
			beginStmt.executeUpdate();
			beginStmt.close();
		} catch (Exception e) {
			logger.error("Error in database initialization: ", e);
		}
	}

	static void initializeReserveHash() {
		reservedHash.put("ALL", "ALL");
		reservedHash.put("ANALYSE", "ANALYSE");
		reservedHash.put("ANALYZE", "ANALYZE");
		reservedHash.put("AND", "AND");
		reservedHash.put("ANY", "ANY");
		reservedHash.put("ARRAY", "ARRAY");
		reservedHash.put("AS", "AS");
		reservedHash.put("ASC", "ASC");
		reservedHash.put("ASYMMETRIC", "ASYMMETRIC");
		reservedHash.put("AUTHORIZATION", "AUTHORIZATION");
		reservedHash.put("BETWEEN", "BETWEEN");
		reservedHash.put("BINARY", "BINARY");
		reservedHash.put("BOTH", "BOTH");
		reservedHash.put("CASE", "CASE");
		reservedHash.put("CAST", "CAST");
		reservedHash.put("CHECK", "CHECK");
		reservedHash.put("CMAX", "CMAX");
		reservedHash.put("COLLATE", "COLLATE");
		reservedHash.put("COLUMN", "COLUMN");
		reservedHash.put("CONSTRAINT", "CONSTRAINT");
		reservedHash.put("CREATE", "CREATE");
		reservedHash.put("CROSS", "CROSS");
		reservedHash.put("CURRENT_DATE", "CURRENT_DATE");
		reservedHash.put("CURRENT_ROLE", "CURRENT_ROLE");
		reservedHash.put("CURRENT_TIME", "CURRENT_TIME");
		reservedHash.put("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP");
		reservedHash.put("CURRENT_USER", "CURRENT_USER");
		reservedHash.put("DEFAULT", "DEFAULT");
		reservedHash.put("DEFERRABLE", "DEFERRABLE");
		reservedHash.put("DESC", "DESC");
		reservedHash.put("DISTINCT", "DISTINCT");
		reservedHash.put("DO", "DO");
		reservedHash.put("ELSE", "ELSE");
		reservedHash.put("END", "END");
		reservedHash.put("EXCEPT", "EXCEPT");
		reservedHash.put("FALSE", "FALSE");
		reservedHash.put("FOR", "FOR");
		reservedHash.put("FOREIGN", "FOREIGN");
		reservedHash.put("FREEZE", "FREEZE");
		reservedHash.put("FROM", "FROM");
		reservedHash.put("FULL", "FULL");
		reservedHash.put("GRANT", "GRANT");
		reservedHash.put("GROUP", "GROUP");
		reservedHash.put("HAVING", "HAVING");
		reservedHash.put("ILIKE", "ILIKE");
		reservedHash.put("IN", "IN");
		reservedHash.put("INITIALLY", "INITIALLY");
		reservedHash.put("INNER", "INNER");
		reservedHash.put("INTERSECT", "INTERSECT");
		reservedHash.put("INTO", "INTO");
		reservedHash.put("IS", "IS");
		reservedHash.put("ISNULL", "ISNULL");
		reservedHash.put("JOIN", "JOIN");
		reservedHash.put("LEADING", "LEADING");
		reservedHash.put("LEFT", "LEFT");
		reservedHash.put("LIKE", "LIKE");
		reservedHash.put("LIMIT", "LIMIT");
		reservedHash.put("LOCALTIME", "LOCALTIME");
		reservedHash.put("LOCALTIMESTAMP", "LOCALTIMESTAMP");
		reservedHash.put("NATURAL", "NATURAL");
		reservedHash.put("NEW", "NEW");
		reservedHash.put("NOT", "NOT");
		reservedHash.put("NOTNULL", "NOTNULL");
		reservedHash.put("NULL", "NULL");
		reservedHash.put("OFF", "OFF");
		reservedHash.put("OFFSET", "OFFSET");
		reservedHash.put("OLD", "OLD");
		reservedHash.put("ON", "ON");
		reservedHash.put("ONLY", "ONLY");
		reservedHash.put("OR", "OR");
		reservedHash.put("ORDER", "ORDER");
		reservedHash.put("OUTER", "OUTER");
		reservedHash.put("OVERLAPS", "OVERLAPS");
		reservedHash.put("PLACING", "PLACING");
		reservedHash.put("PRIMARY", "PRIMARY");
		reservedHash.put("REFERENCES", "REFERENCES");
		reservedHash.put("RIGHT", "RIGHT");
		reservedHash.put("SELECT", "SELECT");
		reservedHash.put("SESSION_USER", "SESSION_USER");
		reservedHash.put("SIMILAR", "SIMILAR");
		reservedHash.put("SOME", "SOME");
		reservedHash.put("SYMMETRIC", "SYMMETRIC");
		reservedHash.put("TABLE", "TABLE");
		reservedHash.put("THEN", "THEN");
		reservedHash.put("TO", "TO");
		reservedHash.put("TRAILING", "TRAILING");
		reservedHash.put("TRUE", "TRUE");
		reservedHash.put("UNION", "UNION");
		reservedHash.put("UNIQUE", "UNIQUE");
		reservedHash.put("USER", "USER");
		reservedHash.put("USING", "USING");
		reservedHash.put("VERBOSE", "VERBOSE");
		reservedHash.put("WHEN", "WHEN");
		reservedHash.put("WHERE", "WHERE");
		reservedHash.put("WINDOW", "WINDOW");
		reservedHash.put("WITH", "WITH");
	}
}

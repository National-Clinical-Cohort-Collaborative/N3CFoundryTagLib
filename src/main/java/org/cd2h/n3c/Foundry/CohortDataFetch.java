package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.LocalProperties;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONObject;

public class CohortDataFetch {
    static boolean load = true;
	static Logger logger = Logger.getLogger(CohortDataFetch.class);
	static LocalProperties prop_file = null;
    static Connection conn = null;

	static Attribute[] attributes = null;
	static Hashtable<String, String> attributeHash = null;
	static Hashtable<String, String> reservedHash = new Hashtable<String, String>();

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("enclave_cohort");
		initializeReserveHash();
		
		JSONObject result = APIRequest.fetchDirectory(prop_file);
		JSONArray  array  = result.getJSONArray("values");
		for (int  i = 0; i < array.length();  i++)  {
			JSONObject element  = array.getJSONObject(i);
			String name  = element.getString("name");
			logger.info("name: " +  name);
			String rid  = element.getString("rid");
			logger.info("\trid:  " +  rid);
			process(name, rid);
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
		attributes = processLabels(contents);
		setTypes(attributes, contents);
		storeData(generateSQLName(tableName), attributes, contents);
	}

	static Attribute[] processLabels(List<?> contents) {
		attributeHash = new Hashtable<String, String>();
		String[] labels = (String[]) contents.get(0);
		Attribute[] attributes = new Attribute[labels.length];

		logger.debug("Labels: " + arrayToString(labels));

		for (int i = 0; i < labels.length; i++) {
			attributes[i] = new Attribute();
			attributes[i].label = generateSQLName(labels[i]);
			attributes[i].index = i;

			if (attributes[i].label.length() > 0 && attributes[i].label.charAt(0) > 255)
				attributes[i].label = attributes[i].label.substring(2);
			if (attributes[i].label.length() > 2 && !Character.isLetterOrDigit(attributes[i].label.charAt(0))) {
				attributes[i].label = attributes[i].label.substring(1);
			}
			if (attributes[i].label.length() == 0) {
				attributes[i].label = "Null_" + i;
			} else if (attributes[i].label.toLowerCase().equals("references")) {
				attributes[i].label += "_" + i;
			} else if (Character.isDigit(attributes[i].label.charAt(0)) || attributes[i].label.charAt(0) == '#') {
				attributes[i].label = "Column_" + attributes[i].label;
			}
		}

		return attributes;
	}

	static void setTypes(Attribute[] attributes, List<?> contents) {
		for (int i = 0; i < attributes.length; i++) {
			setType(attributes[i], contents);
			logger.debug("setting type: " + attributes[i]);
		}
	}

	static void setType(Attribute theAttribute, List<?> contents) {
		if (isType(theAttribute.index, "-?[0-9]*", contents)) {
			if (isInt(theAttribute.index,contents))
				theAttribute.type = "int";
			else
				theAttribute.type = "bigint";
		} else if (isType(theAttribute.index, "(-?[0-9]+([.][0-9]+)?)?", contents))
			theAttribute.type = "float";
		else if (isType(theAttribute.index, "([yY]([eE][sS])?|[nN]([oO])?|[tT]([rR][uU][eE])?|[fF]([aA][lL][sS][eE])?)?", contents))
			theAttribute.type = "boolean";
		else if (isType(theAttribute.index,
				"([0-3]?[0-9]-(J[aA][nN]|F[eE][bB]|M[aA][rR]|A[pP][rR]|M[aA][yY]|J[uU][nN]|J[uU][lL]|A[uU][gG]|S[eE][pP]|O[cC][tT]|N[oO][vV]|D[eE][cC])-[0-9][0-9]([0-9][0-9])?)?",
				contents))
			theAttribute.type = "date";
		else
			theAttribute.type = "text";
	}

	static boolean isType(int index, String pattern, List<?> contents) {
		boolean nonEmpty = false;
		Iterator<?> iterator = contents.iterator();
		iterator.next();
		while (iterator.hasNext()) {
			String[] contentArray = (String[]) iterator.next();
			if (contentArray.length > index && contentArray[index].length() > 0)
				nonEmpty = true;
			logger.debug("pattern: " + pattern + "  element: " + index + " array: " + arrayToString(contentArray));
			if (contentArray.length > index && !Pattern.matches(pattern, contentArray[index]))
				return false;
		}
		return nonEmpty;
	}
	
	static boolean isInt(int index, List<?> contents) {
		try {
			Iterator<?> iterator = contents.iterator();
			iterator.next();
			while (iterator.hasNext()) {
				String[] contentArray = (String[]) iterator.next();
				Integer.parseInt(contentArray[index]);
			}
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
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
					insertStmt.setNull(i + 1, sqlNullType(attributes[i].type));
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

	static int sqlNullType(String type) {
		if (type.equals("int"))
			return java.sql.Types.INTEGER;
		else if (type.equals("bigint"))
			return java.sql.Types.BIGINT;
		else if (type.equals("float"))
			return java.sql.Types.FLOAT;
		else if (type.equals("boolean"))
			return java.sql.Types.BOOLEAN;
		else if (type.equals("date"))
			return java.sql.Types.DATE;
		else
			return java.sql.Types.CHAR;

	}

	static String arrayToString(String[] theArray) {
		StringBuffer theBuffer = new StringBuffer();
		theBuffer.append("[");
		theBuffer.append(theArray[0]);

		for (int i = 1; i < theArray.length; i++)
			theBuffer.append(", " + theArray[i]);

		theBuffer.append("]");
		return theBuffer.toString();
	}

	static String generateSQLName(String attribute) {
		if (attribute == null || attribute.length() == 0)
			return "";
		String attributeBase = attribute.trim().replace("\uFEFF", "");
		attributeBase = (Character.isJavaIdentifierStart(attributeBase.charAt(0)) ? "" : "x___")
				+ attributeBase.replaceAll("[^A-Za-z0-9_]", "_");
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

	static class Attribute {
		String label = null;
		String type = null;
		int index = 0;

		public String toString() {
			return "{" + label + " " + type + "}";
		}
	}
}

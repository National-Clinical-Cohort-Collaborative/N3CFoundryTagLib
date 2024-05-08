package org.cd2h.n3c.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class DatabaseAnalyzer {
	static Logger logger = Logger.getLogger(DatabaseAnalyzer.class);

	Connection conn = null;
	DatabaseMetaData dbMeta = null;
	
	Hashtable<String, Hashtable<String, Vector<Attribute>>> schemaHash = new Hashtable<String, Hashtable<String, Vector<Attribute>>>();
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		LocalProperties prop_file = null;
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		Connection conn = APIRequest.getConnection(prop_file);
		
		DatabaseAnalyzer analyzer = new DatabaseAnalyzer(conn);
		analyzer.initialize("n3c_dashboard_ph");
	}

	public DatabaseAnalyzer(Connection conn) throws SQLException {
		this.conn = conn;
		dbMeta = conn.getMetaData();
	}
	
	public void initialize(String schema) throws SQLException {
		if (schemaHash.containsKey(schema))
			return;
		
		logger.info("initializing " + schema);
		Hashtable<String, Vector<Attribute>> tableHash = new Hashtable<String, Vector<Attribute>>();
		schemaHash.put(schema, tableHash);
		
		String[] types = {"TABLE"};
		ResultSet rs = dbMeta.getTables(null, schema, "%",types);
		while (rs.next()) {
			String table = rs.getString(3);
			logger.info("\ttable: " + table);
			
			Vector<Attribute> attributeVector = new Vector<Attribute>();
			tableHash.put(table, attributeVector);
			
			ResultSet trs = dbMeta.getColumns(null, schema, table, "%");
			while(trs.next()){
				String attribute = trs.getString(4);
				String type = trs.getString(6);
				logger.info("\t\tattribute: " + attribute + " : " + type);
				attributeVector.add(new Attribute(attribute, type));
			}
		}
	}
	
	public boolean compatible(String schema, String table, Attribute[] csvAttributes) throws SQLException {
		logger.info("analyzing " + schema + " : " + table);
		
		if (!schemaHash.containsKey(schema)) {
			initialize(schema);
		}
		
		Hashtable<String, Vector<Attribute>> tableHash = schemaHash.get(schema);
		Vector<Attribute> attributeVector = tableHash.get(table);
		
		if (attributeVector == null)
			return true;
		
		if (attributeVector.size() != csvAttributes.length)
			return false;
		
		boolean result = true;
		for (int i = 0; i < csvAttributes.length; i++) {
			if (!csvAttributes[i].label.toLowerCase().equals(attributeVector.elementAt(i).label)) {
				logger.info("\tlabel mismatch: " + csvAttributes[i] + " : " + attributeVector.elementAt(i));
				result = false;
			}
			switch (attributeVector.elementAt(i).type) {
			case "int4":
				if (!csvAttributes[i].type.equals("int")) {
					logger.info("\ttype mismatch: " + csvAttributes[i] + " : " + attributeVector.elementAt(i));
					result = false;
				}
				break;
			case "int8":
				if (!csvAttributes[i].type.equals("int")) {
					logger.info("\ttype mismatch: " + csvAttributes[i] + " : " + attributeVector.elementAt(i));
					result = false;
				}
				break;
			case "float8":
				if (!csvAttributes[i].type.equals("float")) {
					logger.info("\ttype mismatch: " + csvAttributes[i] + " : " + attributeVector.elementAt(i));
					result = false;
				}
				break;
			case "bool":
				if (!csvAttributes[i].type.equals("bolean")) {
					logger.info("\ttype mismatch: " + csvAttributes[i] + " : " + attributeVector.elementAt(i));
					result = false;
				}
				break;
			case "timestamp":
				if (!csvAttributes[i].type.equals("date")) {
					logger.info("\ttype mismatch: " + csvAttributes[i] + " : " + attributeVector.elementAt(i));
					result = false;
				}
				break;
			default:
				if (!csvAttributes[i].type.equals("text")) {
					logger.info("\ttype mismatch: " + csvAttributes[i] + " : " + attributeVector.elementAt(i));
					result = false;
				}
				break;
			}
		}
		
		return result;
	}
}

package org.cd2h.n3c.Foundry;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.PropertyLoader;
import org.json.JSONArray;
import org.json.JSONObject;

public class QuestionDataFetchNew extends CohortDataFetch {

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("n3c_foundry");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("n3c_questions_new");
		initializeReserveHash();

		PreparedStatement stmt = null;

		if (args.length > 2 && args[2].equals("metrics")) {
			metrics();
			conn.commit();
			return;
		} else if (args.length > 2 && args[2].equals("new")) {
			logger.info("processing only new feeds!");
			stmt = conn.prepareStatement(
					"select rid from palantir.tiger_team_new where active and rid not in (select rid from palantir.tiger_team_file_new)");
		} else if (args.length > 2) {
			logger.info("processing " + args[2]);
			try {
				JSONObject result = APIRequest.fetchDirectory(args[2]);
				process(args[2], result);
				conn.commit();
			} catch (Exception e) {
				logger.error("skipping " + args[2] + " due to error");
			}
			conn.close();
			return;
		} else
			stmt = conn.prepareStatement("select rid from palantir.tiger_team_new where active");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			String compass = rs.getString(1);
			try {
				JSONObject result = APIRequest.fetchDirectory(compass);
				process(compass, result);
				conn.commit();
			} catch (Exception e) {
				logger.error("skipping " + compass + " due to error");
			}
		}

		conn.close();
	}
	
	static void process(String compass, JSONObject result) throws Exception {
		JSONArray  array  = result.getJSONArray("values");
		for (int  i = 0; i < array.length();  i++)  {
			JSONObject element  = array.getJSONObject(i);
			String name  = element.getString("name");
			logger.info("name: " +  name);
			if (name.equals("Copy Approved Downloads"))
				continue;
			String rid  = element.getString("rid");
			logger.info("\trid:  " +  rid);
//			if (name.equals("final_results_with_gender_censored") || name.equals("icd10_individual_symptom_summary_counts") || name.equals("icd10_individual_symptom_summary_counts_by_symptom"))
//				process(name, rid, true);
//			else
//				process(name, rid, false);
			try {
				process(name, rid, true);
			} catch (Exception e) {
				logger.error("skipping " + name + " due to error",e);
			}
			
			PreparedStatement stmt = conn.prepareStatement("update palantir.tiger_team_file_new set updated = now(), metadata = ?::jsonb where rid = ? and file = ?");
			stmt.setString(1, element.toString(3));
			stmt.setString(2, compass);
			stmt.setString(3, generateSQLName(name, true));
			int matched = stmt.executeUpdate();
			stmt.close();
			
			if (matched == 0) {
				stmt = conn.prepareStatement("insert into palantir.tiger_team_file_new values(?, ?, now(), ?::jsonb)");
				stmt.setString(1, compass);
				stmt.setString(2, generateSQLName(name, true));
				stmt.setString(3, element.toString(3));
				stmt.executeUpdate();
				stmt.close();
			}
		}
	}
	
	static void metrics() throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("select file from palantir.tiger_team_file_new where updated is not null and aggregate_column is not null");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			String file = rs.getString(1);
			generateMetrics(file);
		}
		stmt.close();
	}
	
	static void generateMetrics(String fileName) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("select ((metadata->>'modified')::jsonb->>'time')::timestamp as updated,aggregate_column,aggregate_type "
														+ "from palantir.tiger_team_file_new "
														+ "where file = ? "
														+ "  and (file,((metadata->>'modified')::jsonb->>'time')::timestamp) not in (select file,updated from palantir.tiger_team_file_new_metrics)");
		stmt.setString(1, fileName);
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			String updated = rs.getString(1);
			String column = rs.getString(2);
			String type = rs.getString(3);
			logger.info(fileName + " : " + updated + " : " + column + " : " + type);
			
			PreparedStatement updateStmt = null;
			switch(type) {
			case "text":
				updateStmt = conn.prepareStatement(""
							+ "insert into palantir.tiger_team_file_new_metrics "
							+ "select "
							+ "		?,"
							+ "		?::timestamp,"
							+ "		count(*) as rows,"
							+ "		sum(case"
							+ "			when (" + column + " ~ '<20'::text or " + column + " is null) then 0"
							+ "			else " + column + "::int"
							+ "		end) as count "
							+ "from n3c_questions_new." + fileName
							+ "");
				updateStmt.setString(1, fileName);
				updateStmt.setString(2, updated);
				updateStmt.executeUpdate();
				updateStmt.close();
				break;
			case "int":
				updateStmt = conn.prepareStatement(""
						+ "insert into palantir.tiger_team_file_new_metrics "
						+ "select "
						+ "		?,"
						+ "		?::timestamp,"
						+ "		count(*) as rows,"
						+ "		sum(" + column + ") as count "
						+ "from n3c_questions_new." + fileName
						+ "");
				updateStmt.setString(1, fileName);
				updateStmt.setString(2, updated);
				updateStmt.executeUpdate();
				updateStmt.close();
				break;
			case "skip":
				updateStmt = conn.prepareStatement(""
						+ "insert into palantir.tiger_team_file_new_metrics "
						+ "select "
						+ "		?,"
						+ "		?::timestamp,"
						+ "		count(*) as rows,"
						+ "		0 as count "
						+ "from n3c_questions_new." + fileName
						+ "");
				updateStmt.setString(1, fileName);
				updateStmt.setString(2, updated);
				updateStmt.executeUpdate();
				updateStmt.close();
				break;
			default:
				break;
			}
		}
		stmt.close();
		conn.commit();
	}
}

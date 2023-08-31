package org.cd2h.n3c.ConceptSet;

import java.io.FileWriter;
import java.io.IOException;
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
import org.json.JSONObject;

import com.itextpdf.io.font.FontProgram;
import com.itextpdf.io.font.FontProgramFactory;
import com.itextpdf.io.font.PdfEncodings;
import com.itextpdf.io.font.constants.StandardFonts;
import com.itextpdf.io.image.ImageDataFactory;
import com.itextpdf.kernel.colors.ColorConstants;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.font.PdfFontFactory;
import com.itextpdf.kernel.geom.PageSize;
import com.itextpdf.kernel.geom.Rectangle;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfDocumentInfo;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.kernel.pdf.action.PdfAction;
import com.itextpdf.kernel.pdf.annot.PdfLinkAnnotation;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.AreaBreak;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Image;
import com.itextpdf.layout.element.Link;
import com.itextpdf.layout.element.List;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import com.itextpdf.layout.element.Text;
import com.itextpdf.layout.property.TextAlignment;

public class Generator {
	static Logger logger = Logger.getLogger(Generator.class);
	static String pathPrefix = "/usr/local/CD2H/lucene/";
	static LocalProperties prop_file = null;
    static Connection conn = null;

    static Document document = null;
    static FontProgram fontProgram = null;
    static PdfFont font = null;
    
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, InterruptedException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("concept_sets");
		pathPrefix = prop_file.getProperty("md_path");
		conn = getConnection();
		
		fontProgram = FontProgramFactory.createFont("/fonts/Symbola.ttf", true);

		generateConceptSetIText();
	}
	
	static JSONObject generateJSON(int id) {
		JSONObject obj = new JSONObject();
		obj.append("id", 1234);
		JSONObject expr = new JSONObject();
		JSONObject items = new JSONObject();
		
		obj.append("expression", expr);
		return obj;
	}
	
	static void generateConceptSetIText() throws IOException, SQLException {
		int count = 0;
		PreparedStatement stmt = conn.prepareStatement("select"
				+ "					codeset_id,"
				+ "					alias,"
				+ "					intention,"
				+ "					version,"
				+ "					is_most_recent_version,"
				+ "					update_message,"
				+ "					provisional_approval_date,"
				+ "					release_name,"
				+ "					coalesce(name, first_name||' '||last_name) as author,"
				+ "					limitations,"
				+ "					issues,"
				+ "					provenance,"
				+ "					concept_json.json,"
				+ "					set_type"
				+ "				from enclave_concept.concept_set_display join enclave_concept.concept_json"
				+ "					on (concept_set_display.codeset_id=concept_json.codeset_id)");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			count++;
			int id = rs.getInt(1);
			String alias = rs.getString(2);
			String intention = rs.getString(3);
			String version = rs.getString(4);
			String is_most_recent_version = rs.getString(5);
			String update_message = rs.getString(6);
			String provisional_approval_date = rs.getString(7);
			String release_name = rs.getString(8);
			String created_by = rs.getString(9);
			String limitations = rs.getString(10);
			String issues = rs.getString(11);
			String provenance = rs.getString(12);
			String json = rs.getString(13);
			String set_type = rs.getString(14);
			logger.info(id + " : " + alias);

			if (alias == null)
				continue;

			if (json != null) {
				FileWriter myWriter = new FileWriter(pathPrefix + id + ".json");
				myWriter.write(json+ "\n");
				myWriter.close();
			}

		    PdfWriter writer = new PdfWriter(pathPrefix + id + ".pdf");
			PdfDocument pdfDoc = new PdfDocument(writer);
			font = PdfFontFactory.createFont(fontProgram, PdfEncodings.IDENTITY_H);
			
			PdfDocumentInfo info = pdfDoc.getDocumentInfo();
			info.setAuthor("National COVID-19 Cohort Collaborative");
			info.setTitle("Concept Set Definition: " + id);
			info.setCreator("N3C Labs");
			info.setSubject("concept sets");

			pdfDoc.addNewPage();

			// Creating a Document
			document = new Document(pdfDoc, PageSize.LETTER);
			
			Paragraph lead = new Paragraph();
			Image image = new Image(ImageDataFactory.create("/Users/eichmann/Documents/Components/workspace/N3CFoundryTagLib/src/main/resources/images/n3c_logo_narrow.png"));
			image.setHeight(64);
//			image.setWidth(64);
			lead.add(image);
			document.add(lead);
			
			Paragraph head = new Paragraph();
			Text header = null;
			if (set_type.equals("user"))
				header = new Text("N3C Research Team Code Set: " + alias);
			else
				header = new Text("N3C Recommended Code Set: " + alias);
			header.setFontSize(18);
			header.setBold();
			head.add(header);
			head.setMarginBottom(25);
			document.add(head);

			addItem("Code Set ID", id+"");
			addItem("Intention", intention);
			addItem("Version", version);
			addItem("Flag for Most Recent", is_most_recent_version);
			addItem("Update Note", update_message);
			addItem("Approval Date", provisional_approval_date);
			addItem("Release Name", release_name);
			addItem("Author", created_by);
			addItem("Project Name(s)", "");
			
			List projects = new List();
			PreparedStatement substmt = conn.prepareStatement("select research_project_id,title from enclave_concept.concept_set_project where codeset_id = ?");
			substmt.setInt(1, id);
			ResultSet subrs = substmt.executeQuery();
			while (subrs.next()) {
				String pid = subrs.getString(1);
				String title = subrs.getString(2);
				projects.add(pid + " : " + title);
			}
			substmt.close();
			document.add(projects);

			addItem("Description", limitations);
			addItem("Issues", issues);
			addItem("Provenance", provenance);
			
			substmt = conn.prepareStatement("select doi from enclave_concept.zenodo_deposit where codeset_id = ?");
			substmt.setInt(1, id);
			subrs = substmt.executeQuery();
			while (subrs.next()) {
				String doi = subrs.getString(1);
				addItem("DOI", doi);
			}
			substmt.close();

			Paragraph ack = new Paragraph();
			Text ackLabel = new Text("Acknowledgements: ");
			ackLabel.setBold();
			ack.add(ackLabel);
			Text link = new Text("https://covid.cd2h.org/acknowledgements");
			ack.add(link);
			ack.setMarginTop(100);
			document.add(ack);
			
			Paragraph boiler = new Paragraph();
			Text boilerplate = new Text("This code set was prepared for use with the NCATS N3C Data Enclave "
					+ "(https://covid.cd2h.org/enclave) and supported by NCATS U24 TR002306. This code set does "
					+ "not contain any patient data. The enclave itself is made possible by the patients whose "
					+ "information is included within the data from participating organizations "
					+ "(https://covid.cd2h.org/dtas) and the organizations and scientists (https://covid.cd2h.org/duas) who "
					+ "have contributed to the on-going development of this community resource "
					+ "(https://doi.org/10.1093/jamia/ocaa196).");
			boiler.add(boilerplate);
			document.add(boiler);

			document.add(new AreaBreak());
			addItem("Logic Expression", "");

			// Creating a table
			Table table = new Table(8).useAllAvailableWidth();

			// Adding cells to the table
			addHeaderCell(table, "Concept ID");
			addHeaderCell(table, "Concept Code");
			addHeaderCell(table, "Concept Name");
			addHeaderCell(table, "Domain");
			addHeaderCell(table, "Standard Concept");
			addHeaderCell(table, "Exclude");
			addHeaderCell(table, "Descendants");
			addHeaderCell(table, "Mapped");
			
			PreparedStatement inclstmt = conn.prepareStatement("select concept_id,concept_code,concept_name,domain_id,standard_concept,is_excluded,include_descendants,include_mapped from enclave_concept.code_set_concept where codeset_id = ?");
			inclstmt.setInt(1, id);
			ResultSet inclrs = inclstmt.executeQuery();
			while (inclrs.next()) {
				int concept_id = inclrs.getInt(1);
				String concept_code = inclrs.getString(2);
				String concept_name = inclrs.getString(3);
				String domain = inclrs.getString(4);
				String standard_concept = inclrs.getString(5);
				boolean exclude = inclrs.getBoolean(6);
				boolean decendants = inclrs.getBoolean(7);
				boolean mapped = inclrs.getBoolean(8);
				addCell(table, concept_id + "", TextAlignment.RIGHT);
				addCell(table, concept_code, TextAlignment.RIGHT);
				addCell(table, concept_name);
				addCell(table, domain);
				addCell(table, standard_concept);
				addCheckCell(table, exclude ? "\u2611" : "\u2610", TextAlignment.CENTER);
				addCheckCell(table, decendants ? "\u2611" : "\u2610", TextAlignment.CENTER);
				addCheckCell(table, mapped ? "\u2611" : "\u2610", TextAlignment.CENTER);

			}
			inclstmt.close();

			// Adding Table to document
			document.add(table);

			document.add(new AreaBreak());
			addItem("Included Concepts", "");

			// Creating a table
			Table table2 = new Table(2).useAllAvailableWidth();

			// Adding cells to the table
			addHeaderCell(table2, "Concept ID");
			addHeaderCell(table2, "Concept Name");
			
			inclstmt = conn.prepareStatement("select concept_id,concept_name from enclave_concept.concept_set_members where codeset_id = ?");
			inclstmt.setInt(1, id);
			inclrs = inclstmt.executeQuery();
			while (inclrs.next()) {
				int concept_id = inclrs.getInt(1);
				String concept_name = inclrs.getString(2);
				addCell(table2, concept_id + "", TextAlignment.RIGHT);
				addCell(table2, concept_name);

			}
			inclstmt.close();

			// Adding Table to document
			document.add(table2);

			document.add(new AreaBreak());
			addItem("JSON", "");
			if (json != null)
				document.add(new Paragraph(json.replaceAll(" ", "\u00A0"))
						.setFont(PdfFontFactory.createFont(StandardFonts.COURIER)).setFontSize(9));
			document.close();
		}
		stmt.close();
		
		logger.info("concepts indexed: " + count);
	}
	
	static Link generateLink(String anchor, String URI) {
		Rectangle rect = new Rectangle(0, 0); 
		PdfLinkAnnotation annotation = new PdfLinkAnnotation(rect); 
		PdfAction action = PdfAction.createURI(URI); 
		annotation.setAction(action); 
		Link link = new Link(anchor,annotation);
		return link;
	}
	
	static void addItem(String label, String value) {
		Text header = new Text(label + ": ");
		header.setBold();
		Text body = new Text(value != null ? value : "");
		Paragraph para = new Paragraph();
		para.add(header);
		para.add(body);
		document.add(para);
	}
	
	static void addHeaderCell(Table table, String content) {
		addHeaderCell(table, content, TextAlignment.LEFT);
	}
	
	static void addHeaderCell(Table table, String content, TextAlignment alignment) {
		Paragraph paragraph = new Paragraph(content != null ? content : "");
		paragraph.setFontSize(9);
		paragraph.setBold();
		Cell cell = new Cell().add(paragraph);
		cell.setTextAlignment(alignment);
		cell.setBackgroundColor(ColorConstants.LIGHT_GRAY);
		table.addHeaderCell(cell);
	}
	
	static void addCell(Table table, String content) {
		addCell(table, content, TextAlignment.LEFT);
	}
	
	static void addCell(Table table, String content, TextAlignment alignment) {
		Paragraph paragraph = new Paragraph(content != null ? content : "");
		paragraph.setFontSize(9);
		Cell cell = new Cell().add(paragraph);
		cell.setTextAlignment(alignment);
		table.addCell(cell);
	}
	
	static void addCheckCell(Table table, String content, TextAlignment alignment) {
		Paragraph paragraph = new Paragraph();
		paragraph.setFont(font);
		paragraph.setFontSize(9);
		paragraph.add(content != null ? content : "");
		Cell cell = new Cell().add(paragraph);
		cell.setTextAlignment(alignment);
		table.addCell(cell);
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

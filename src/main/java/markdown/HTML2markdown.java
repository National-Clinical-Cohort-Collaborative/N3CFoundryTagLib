package markdown;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.util.APIRequest;
import org.cd2h.n3c.util.LocalProperties;
import org.cd2h.n3c.util.PropertyLoader;

import com.vladsch.flexmark.html2md.converter.*;
import com.vladsch.flexmark.util.data.MutableDataSet;

public class HTML2markdown  {
    static Logger logger = Logger.getLogger(HTML2markdown.class);
    static LocalProperties prop_file = null;
	static Connection conn = null;
	static boolean load = true;

	static MutableDataSet options = null;
	static FlexmarkHtmlConverter converter = null;
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		PropertyConfigurator.configure(args[0]);
		prop_file = PropertyLoader.loadProperties("markdown");
		conn = APIRequest.getConnection(prop_file);
		conn.setSchema("enclave_concept");

		options = new MutableDataSet();
			//         .set(Parser.EXTENSIONS, Collections.singletonList(HtmlConverterTextExtension.create()));
     	converter = FlexmarkHtmlConverter.builder(options).build();
     	
     	map("press_release","description");
     	//map("webinar","topic");
     	//test();
	}
	
	static void map(String table, String column) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("select nid,"+column+" from drupal."+table);
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int nid = rs.getInt(1);
			String html = rs.getString(2);
			String markdown = converter.convert(html);
			logger.info(nid);
			logger.info(html);
			logger.info(markdown);
			
			PreparedStatement update = conn.prepareStatement("update drupal."+table+" set "+column+ " = ? where nid = ?");
			update.setString(1, markdown);
			update.setInt(2, nid);
			update.executeUpdate();
			update.close();
		}
		stmt.close();
		conn.commit();
	}
	
	static void test() {
	    String html = "<ul>\n" +
	            "  <li>\n" +
	            "    <p>Add: live templates starting with <code>.</code> <kbd>Kbd</kbd> <a href='http://example.com'>link</a></p>\n" +
	            "    <table>\n" +
	            "      <thead>\n" +
	            "        <tr><th> Element       </th><th> Abbreviation    </th><th> Expansion                                               </th></tr>\n" +
	            "      </thead>\n" +
	            "      <tbody>\n" +
	            "        <tr><td> Abbreviation  </td><td> <code>.abbreviation</code> </td><td> <code>*[]:</code>                                                 </td></tr>\n" +
	            "        <tr><td> Code fence    </td><td> <code>.codefence</code>    </td><td> ``` ... ```                                       </td></tr>\n" +
	            "        <tr><td> Explicit link </td><td> <code>.link</code>         </td><td> <code>[]()</code>                                                  </td></tr>\n" +
	            "      </tbody>\n" +
	            "    </table>\n" +
	            "  </li>\n" +
	            "</ul>";
	    String markdown = converter.convert(html);

	    logger.info("HTML:");
	    logger.info(html);

	    logger.info("Markdown:");
	    logger.info(markdown);
	}

}

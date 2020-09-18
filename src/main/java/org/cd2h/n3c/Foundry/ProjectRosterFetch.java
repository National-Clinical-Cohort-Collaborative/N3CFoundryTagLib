package org.cd2h.n3c.Foundry;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.cd2h.n3c.Foundry.util.APIRequest;
import org.cd2h.n3c.Foundry.util.LocalProperties;
import org.cd2h.n3c.Foundry.util.PropertyLoader;
import org.json.JSONObject;

public class ProjectRosterFetch {
    static Logger logger = Logger.getLogger(ProjectRosterFetch.class);
    static LocalProperties prop_file = null;

    public static void main(String[] args) throws IOException {
	PropertyConfigurator.configure(args[0]);
	prop_file = PropertyLoader.loadProperties("n3c_foundry");

	JSONObject results = APIRequest.submit(prop_file, "n3c-website-approved-projects");
	logger.info("results:\n" + results.toString(3));
    }

}

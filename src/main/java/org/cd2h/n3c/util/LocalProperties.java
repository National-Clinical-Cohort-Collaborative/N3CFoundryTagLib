package org.cd2h.n3c.util;

import java.util.Properties;

@SuppressWarnings("serial")
public class LocalProperties extends Properties {
    
    public int getIntProperty(String property) {
	if (super.getProperty(property) == null)
	    return 0;
	
	return Integer.parseInt(super.getProperty(property));
    }

    public boolean getBooleanProperty(String property) {
	if (super.getProperty(property) == null)
	    return false;
	
	return Boolean.parseBoolean(super.getProperty(property));
    }

    public boolean getBooleanProperty(String property, boolean theDefault) {
	if (super.getProperty(property) == null)
	    return theDefault;
	
	return Boolean.parseBoolean(super.getProperty(property));
    }

}

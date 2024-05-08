package org.cd2h.n3c.util;

public class Attribute {
	public String label = null;
	public String type = null;
	public int index = 0;
	
	public Attribute() {
		
	}
	
	public Attribute(String label, String type) {
		this.label = label;
		this.type = type;
	}

	public Attribute(String type, int index) {
		this.type = type;
		this.index = index;
	}

	public Attribute(String label, String type, int index) {
		this.label = label;
		this.type = type;
		this.index = index;
	}

	public String toString() {
		return "{" + label + " " + type + "}";
	}
}

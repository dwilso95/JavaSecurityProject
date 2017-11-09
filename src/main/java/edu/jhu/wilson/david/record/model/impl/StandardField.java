package edu.jhu.wilson.david.record.model.impl;

import edu.jhu.wilson.david.record.model.Field;

public class StandardField implements Field {

	private String name;
	private String value;
	private String visibility;

	public StandardField(final String name, final String value, final String visibility) {
		this.name = name;
		this.value = value;
		this.visibility = visibility;
	}

	public String getName() {
		return this.name;
	}

	public String getValue() {
		return this.value;
	}

	public String getVisibility() {
		return this.visibility;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}

}

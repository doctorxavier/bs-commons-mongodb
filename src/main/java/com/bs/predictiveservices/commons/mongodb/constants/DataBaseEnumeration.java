package com.bs.predictiveservices.commons.mongodb.constants;

public enum DataBaseEnumeration {

	BSABADELL("bsabadell"),

	BSONLINE("bsonline"),

	MAINFRAME("mainframe"),

	OFFICE_VIEW("office_view");

	private String id;

	private DataBaseEnumeration(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

}

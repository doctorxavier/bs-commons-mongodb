package com.bs.predictiveservices.commons.mongodb.constants;

public enum CollectionEnumeration {

	/* MASTER */
	DIM_BSONLINE_CHANNELS("dim_bsonline_channels"),

	DIM_EMPLOYEE("dim_employee"),

	DIM_OFFICE("dim_office"),

	DIM_OPERATIONAL_MONITORING("dim_operational_monitoring"),

	DIM_OPERATIONS("dim_operations"),

	DIM_ACCESS_CONTROL("dim_access_control"),

	/* FACT */
	BSONLINE_FAC_OPERATION_METRICS("fac_operation_metrics"),

	FAC_OPERATIONS_AGGREGATED("fac_operations_aggregated"),

	FAC_RECONNECTIONS("fac_reconnections"),

	/* RAW */
	OPERATIONAL_MONITORING("operational_monitoring");

	private String id;

	private CollectionEnumeration(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

}

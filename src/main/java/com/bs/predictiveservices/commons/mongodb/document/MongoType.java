package com.bs.predictiveservices.commons.mongodb.document;

public enum MongoType
{
    DOUBLE(1),
    STRING(2),
    OBJECT(3),
    ARRAY(4),
    BINARY(5),
    @Deprecated UNDEFINED(6),
    OBJECT_ID(7),
    BOOLEAN(8),
    DATE(9),
    NULL(10),
    REGEX(11),
    JAVASCRIPT(13),
    SYMBOL(14),
    JAVASCRIPT_WITH_SCOPE(15),
    INT32(16),
    TIMESTAMP(17),
    INT64(18),
    MIN_KEY(255),
    MAX_KEY(127);

    private Integer code;

    public Integer getCode()
    {
        return this.code;
    }

    private MongoType(Integer code) { this.code = code; };
}

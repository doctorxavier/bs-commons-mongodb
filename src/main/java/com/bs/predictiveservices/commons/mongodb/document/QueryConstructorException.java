package com.bs.predictiveservices.commons.mongodb.document;

public class QueryConstructorException extends RuntimeException
{
	private static final long serialVersionUID = 8471668373587359418L;
	public static Integer INVALID_ARRAY_SIZE = -1;
    public static Integer DUPLICATED_KEY = -2;
    public static Integer INVALID_TYPE_IN_MATH_OPERATION = -3;

    private Integer errorCode;
    
    private QueryConstructorException(String msg, Integer errorCode)
    {
        super(msg);

        this.errorCode = errorCode;
    }
    
    public Integer getErrorCode()
    {
        return this.errorCode;
    }

    public static QueryConstructorException invalidArraySizeInEq(int size)
    {
        return new QueryConstructorException("$eq needs an array with exactly 2 values. " +
                "Passed an array with "+size+" values.", INVALID_ARRAY_SIZE);
    }

    public static QueryConstructorException duplicatedKey(String key)
    {
        return new QueryConstructorException("Duplicated key '"+key+"' when merging multiple documents.",
                DUPLICATED_KEY);
    }

    public static QueryConstructorException invalidTypeInMathOperation(Class<?> type)
    {
        return new QueryConstructorException("Type '"+type.getName()+"' is not a valid type for math operation. " +
                "Valid types:  Long, Double, Float, Integer, DBObject or String (to refer to a field name). " +
                "Example: c.divide(\"num\", 2) will be traduced to {$divide: [\"$num\", 2]}.",
                INVALID_TYPE_IN_MATH_OPERATION);
    }
}

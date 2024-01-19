package com.bs.predictiveservices.commons.mongodb.document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class QueryConstructor
{
    public static Logger logger = LoggerFactory.getLogger(QueryConstructor.class);

    private Boolean exception;

    // MONGODB LITERALS

    public static final String $ = "$";

    public static final String AND = "$and";
    public static final String OR = "$or";
    public static final String NOR = "$nor";
    public static final String NOT = "$not";

    public static final String COND = "$cond";
    public static final String IF_NULL = "$ifNull";

    public static final String IN = "$in";
    public static final String NIN = "$nin";
    public static final String EQ = "$eq";
    public static final String CMP = "$cmp";
    public static final String NE = "$ne";
    public static final String LT = "$lt";
    public static final String LTE = "$lte";
    public static final String GT = "$gt";
    public static final String GTE = "$gte";

    public static final String ADD = "$add";
    public static final String SUBTRACT = "$subtract";
    public static final String MULTIPLY = "$multiply";
    public static final String DIVIDE = "$divide";
    public static final String MOD = "$mod";

    public static final String SET = "$set";
    public static final String UNSET = "$unset";
    public static final String INC = "$inc";

    public static final String _ID = "_id";
    public static final String SUM = "$sum";
    public static final String AVG = "$avg";
    public static final String MAX = "$max";
    public static final String MIN = "$min";
    public static final String FIRST = "$first";
    public static final String LAST = "$last";
    public static final String PUSH = "$push";
    public static final String PULL = "$pull";
    public static final String ADD_TO_SET = "$addToSet";

    public static final String EXISTS = "$exists";
    public static final String TYPE = "$type";

    public static final String CONCAT = "$concat";
    public static final String SUBSTR = "$substr";
    public static final String TO_LOWER = "$toLower";
    public static final String TO_UPPER = "$toUpper";
    public static final String STRCASECMP = "$strcasecmp";

    public static final String DAY_OF_YEAR = "$dayOfYear";
    public static final String DAY_OF_MONTH = "$dayOfMonth";
    public static final String DAY_OF_WEEK = "$dayOfWeek";
    public static final String YEAR = "$year";
    public static final String MONTH = "$month";
    public static final String WEEK = "$week";
    public static final String HOUR = "$hour";
    public static final String MINUTE = "$minute";
    public static final String SECOND = "$second";
    public static final String MILLISECOND = "$millisecond";

    public static final String MAP = "$map";
    public static final String LET = "$let";
    public static final String LITERAL = "$literal";

    public static final String SIZE = "$size";

    public static final String MATCH = "$match";
    public static final String GROUP = "$group";
    public static final String PROJECT = "$project";
    public static final String LIMIT = "$limit";
    public static final String SORT = "$sort";

    public static final Integer ASC = 1;
    public static final Integer DESC = -1;
    public static final String META = "$meta";
    public static final String TEXTSCORE = "$textScore";

    // CONSTRUCTORS

    public QueryConstructor()
    {
        this.exception = false;
    }

    public QueryConstructor(Boolean exception)
    {
        this.exception = exception;
    }

    private void launchException(QueryConstructorException e)
    {
        if (this.exception)
        {
            throw e;
        }
        else
        {
            logger.warn(e.getMessage(), e);
        }
    }

    // DOCUMENT CONSTRUCTORS

    public DBObject field(String key, Object value)
    {
        return new BasicDBObject(key, value);
    }

    public DBObject document(Map<String, Object> map)
    {
        DBObject object = new BasicDBObject();

        for (Map.Entry<String, Object> field: map.entrySet())
        {
            object.put(field.getKey(), field.getValue());
        }

        return object;
    }

    public DBObject document(DBObject... documents)
    {
        DBObject finalDocument = new BasicDBObject();
        for (DBObject document: documents)
        {
            for (String key: document.keySet())
            {
                Object value =  document.get(key);
                if (finalDocument.containsField(key))
                {
                    if (value instanceof DBObject)
                    {
                        DBObject currentValue = (DBObject) finalDocument.get(key);
                        currentValue.putAll((DBObject) value);
                        value = currentValue;
                    }
                    else
                    {
                        this.launchException(QueryConstructorException.duplicatedKey(key));
                    }
                }
                finalDocument.put(key, value);
            }
        }
        return finalDocument;
    }

    public DBObject document(List<DBObject> documents)
    {
        return this.document(documents.toArray(new DBObject[documents.size()]));
    }

    public List<DBObject> pipeline(DBObject... operations)
    {
        ArrayList<DBObject> pipeline = new ArrayList<DBObject>();
        for (DBObject operation: operations)
        {
            pipeline.add(operation);
        }
        return pipeline;
    }

    // DATE EXPRESSIONS

    private DBObject dateExpression(String expr, String dateField)
    {
        return this.field(expr, $+dateField);
    }

    public DBObject dayOfYear(String dateField)
    {
        return this.dateExpression(DAY_OF_YEAR, dateField);
    }

    public DBObject dayOfWeek(String dateField)
    {
        return this.dateExpression(DAY_OF_WEEK, dateField);
    }

    public DBObject dayOfMonth(String dateField)
    {
        return this.dateExpression(DAY_OF_MONTH, dateField);
    }

    public DBObject year(String dateField)
    {
        return this.dateExpression(YEAR, dateField);
    }

    public DBObject month(String dateField)
    {
        return this.dateExpression(MONTH, dateField);
    }

    public DBObject week(String dateField)
    {
        return this.dateExpression(WEEK, dateField);
    }

    public DBObject hour(String dateField)
    {
        return this.field(HOUR, $+dateField);
    }

    public DBObject minute(String dateField)
    {
        return this.dateExpression(MINUTE, dateField);
    }

    public DBObject second(String dateField)
    {
        return this.dateExpression(SECOND, dateField);
    }

    public DBObject millisecond(String dateField)
    {
        return this.dateExpression(MILLISECOND, dateField);
    }

    // UPDATE OPERATIONS

    public DBObject set(DBObject... values)
    {
        return this.field(SET, this.document(values));
    }

    public DBObject unset(DBObject... values)
    {
        return this.field(UNSET, this.document(values));
    }

    public DBObject inc(DBObject... values)
    {
        return this.field(INC, this.document(values));
    }

    // AGGREGATION STAGES

    private DBObject aggregationStage(String stage, DBObject... documents)
    {
        return this.field(stage, this.document(documents));
    }

    private DBObject aggregationStage(String stage, List<DBObject> documents)
    {
        return this.field(stage, this.document(documents));
    }

    public DBObject match(DBObject... documents)
    {
        return this.aggregationStage(MATCH, documents);
    }

    public DBObject match(List<DBObject> documents)
    {
        return this.aggregationStage(MATCH, documents);
    }

    public DBObject group(DBObject... documents)
    {
        return this.aggregationStage(GROUP, documents);
    }

    public DBObject group(List<DBObject> documents)
    {
        return this.aggregationStage(GROUP, documents);
    }

    public DBObject project(DBObject... documents)
    {
        return this.aggregationStage(PROJECT, documents);
    }

    public DBObject project(List<DBObject> documents)
    {
        return this.aggregationStage(PROJECT, documents);
    }

    public DBObject sort(DBObject... documents)
    {
        return this.aggregationStage(SORT, documents);
    }

    public DBObject sort(List<DBObject> documents)
    {
        return this.aggregationStage(SORT, documents);
    }

    public DBObject limit(int limit)
    {
        return this.field(LIMIT, limit);
    }

    // AGGREGATION OPERATIONS

    public DBObject _id(String field)
    {
        return this.field(_ID, $ + field);
    }

    public DBObject _id(String... set)
    {
        DBObject _id = new BasicDBObject();
        for (String field: set)
        {
            _id.put(field, $+field);
        }
        return this.field(_ID, _id);
    }

    public DBObject _id(DBObject field)
    {
        return this.field(_ID, field);
    }

    private Object getGroupOperationValue(Object value)
    {
        if (value instanceof String)
        {
            return $+value;
        }
        return value;
    }

    public DBObject sum(Object value)
    {
        return this.field(SUM, this.getGroupOperationValue(value));
    }

    public DBObject avg(Object value)
    {
        return this.field(AVG, this.getGroupOperationValue(value));
    }

    public DBObject max(Object fieldToMax)
    {
        return this.field(MAX, this.getGroupOperationValue(fieldToMax));
    }

    public DBObject min(Object fieldToMin)
    {
        return this.field(MIN, this.getGroupOperationValue(fieldToMin));
    }

    // MATH OPERATIONS

    private Object getMathOperationValue(Object value)
    {
        if (value instanceof String)
        {
            return $+value;
        }
        else if (!(value instanceof DBObject ||value instanceof Integer || value instanceof Double ||value instanceof Float ||value instanceof Long))
        {
            this.launchException(QueryConstructorException.invalidTypeInMathOperation(value.getClass()));
        }
        return value;
    }

    public DBObject multiply(Object... valuesToMultiply)
    {
        BasicDBList listOfValues = new BasicDBList();
        for (Object value: valuesToMultiply)
        {
            listOfValues.add(this.getMathOperationValue(value));
        }
        return this.field(MULTIPLY, listOfValues);
    }

    public DBObject divide(Object numerator, Object denominator)
    {
        BasicDBList values = new BasicDBList();
        values.add(this.getMathOperationValue(numerator));
        values.add(this.getMathOperationValue(denominator));
        return this.field(DIVIDE, values);
    }

    public DBObject subtract(Object expression1, Object expression2)
    {
        BasicDBList values = new BasicDBList();
        values.add(this.getMathOperationValue(expression1));
        values.add(this.getMathOperationValue(expression2));
        return this.field(SUBTRACT, values);
    }

    public DBObject add(Object... valuesToAdd)
    {
        BasicDBList listOfValues = new BasicDBList();
        for (Object value: valuesToAdd)
        {
            listOfValues.add(this.getMathOperationValue(value));
        }
        return this.field(ADD, listOfValues);
    }
    
    // LOGICAL OPERATORS

    public DBObject and(DBObject... conditions)
    {
        return this.field(AND, conditions);
    }

    public DBObject and(List<DBObject> conditions)
    {
        return this.field(AND, conditions);
    }

    public DBObject or(DBObject... conditions)
    {
        return this.field(OR, conditions);
    }

    public DBObject or(List<DBObject> conditions)
    {
        return this.field(OR, conditions);
    }

    public DBObject nor(DBObject... conditions)
    {
        return this.field(NOR, conditions);
    }

    public DBObject nor(List<DBObject> conditions)
    {
        return this.field(NOR, conditions);
    }

    public DBObject not(DBObject... conditions)
    {
        return this.field(NOT, this.document(conditions));
    }

    public DBObject not(List<DBObject> conditions)
    {
        return this.field(NOT, conditions);
    }

    // ELEMENT OPERATORS

    public DBObject exists(Boolean condition)
    {
        return this.field(EXISTS, condition);
    }

    public DBObject type( MongoType type)
    {
        return this.field(TYPE, type.getCode());
    }

    // COMPARISON OPERATORS

    private DBObject compareOperation(String compare, Object value)
    {
        return this.field(compare, value);
    }

    public DBObject lt(Object value)
    {
        return this.compareOperation(LT, value);
    }

    public DBObject lte(Object value)
    {
        return this.compareOperation(LTE, value);
    }

    public DBObject gt(Object value)
    {
        return this.compareOperation(GT, value);
    }

    public DBObject gte(Object value)
    {
        return this.compareOperation(GTE, value);
    }

    public DBObject ne(Object value)
    {
        return this.compareOperation(NE, value);
    }

    private DBObject listOperation(String operation, Object... values)
    {
        BasicDBList list = new BasicDBList();
        for (Object value: values)
        {
            if (value instanceof Collection)
            {
                list.addAll((Collection<?>) value);
            }
            else
            {
                list.add(value);
            }
        }
        return this.field(operation, list);
    }
    
    public DBObject conditionComparison(String comparison, Object... values)
    {
        if (values.length != 2)
        {
            this.launchException(QueryConstructorException.invalidArraySizeInEq(values.length));
        }
        return this.listOperation(comparison, values);
    }

    public DBObject eq(Object... values)
    {
        return this.conditionComparison(EQ, values);
    }
    
    public DBObject ne(Object... values)
    {
        return this.conditionComparison(NE, values);
    }
    
    public DBObject gt(Object... values)
    {
        return this.conditionComparison(GT, values);
    }
    
    public DBObject gte(Object... values)
    {
        return this.conditionComparison(GTE, values);
    }
    
    public DBObject lt(Object... values)
    {
        return this.conditionComparison(LT, values);
    }
    
    public DBObject lte(Object... values)
    {
        return this.conditionComparison(LTE, values);
    }

    public DBObject in(Object... values)
    {
        return this.listOperation(IN, values);
    }

    public DBObject nin(Object... values)
    {
        return this.listOperation(NIN, values);
    }

    public DBObject cond(DBObject condition, Object ifTrue, Object ifFalse) 
    {
    	return this.listOperation(COND, new Object[] { condition, ifTrue, ifFalse });
    }

    // SORT FIELDS

    public DBObject asc(String field)
    {
        return this.field(field, ASC);
    }

    public DBObject desc(String field)
    {
        return this.field(field, DESC);
    }

    public DBObject meta(String metaKey)
    {
        return this.field(META, TEXTSCORE);
    }

    // STRING OPERATIONS

    public DBObject concat(String... values)
    {
        return this.listOperation(CONCAT, (Object[]) values);
    }
    
    public DBObject substr(String string, int init, int length)
    {
    	return this.listOperation(SUBSTR, new Object[] { string, init, length });
    }

    // PROJECT OPERATIONS

    public DBObject literal(Object value)
    {
        return this.field(LITERAL, value);
    }

    public DBObject rename(String oldName, String newName)
    {
        return this.field(newName, $ + oldName);
    }
}

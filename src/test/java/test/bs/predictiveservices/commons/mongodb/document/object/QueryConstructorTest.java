package test.bs.predictiveservices.commons.mongodb.document.object;

import com.bs.predictiveservices.commons.mongodb.document.QueryConstructor;
import org.junit.Before;
import org.junit.Test;

public class QueryConstructorTest
{
    private QueryConstructor c;

    @Before
    public void setUp()
    {
        c = new QueryConstructor(true);
    }

    @Test
    public void printQuery()
    {
    	String office = "0123";
        System.out.println(
			c.pipeline( 
				c.match( c.or(
					c.document(c.field("office", office), c.field("application", "sibis"), c.field("operation", "OPE_REI")),
					c.document(c.field("office", office), c.field("application", "proteo"), c.field("operation", "reintContrato")),
					c.document(c.field("office", office), c.field("application", "sibis"), c.field("operation", "OPE_ENT")),
					c.document(c.field("office", office), c.field("application", "proteo"), c.field("operation", "ingContrato")))),
				c.group( c._id("date", "operation"),
					c.field("total_exec",  c.sum("total_exec")), c.field("total_avg", c.sum(c.multiply("total_exec", "total_avg")))),
				c.match(c.field("total_exec", c.ne(0))),
				c.project(c.field("_id", false), c.rename("_id.date", "date"), c.rename("_id.operation", "operation"), 
					c.field("total_exec", true), c.field("total_avg", c.divide("total_avg", "total_exec"))), 
				c.group(c._id("date"),
					c.field("ingr_sibis_execs", c.max(c.cond(c.eq("$operation", "OPE_ENT"), "$total_exec", 0))),
					c.field("ingr_proteo_execs", c.max(c.cond(c.eq("$operation", "ingContrato"), "$total_exec", 0))),
					c.field("reint_sibis_execs", c.max(c.cond(c.eq("$operation", "OPE_REI"), "$total_exec", 0))),
					c.field("reint_proteo_execs", c.max(c.cond(c.eq("$operation", "reintContrato"), "$total_exec", 0))),
					c.field("ingr_sibis_avg", c.max(c.cond(c.eq("$operation", "OPE_ENT"), "$total_avg", 0))),
					c.field("ingr_proteo_avg", c.max(c.cond(c.eq("$operation", "ingContrato"), "$total_avg", 0))),
					c.field("reint_sibis_avg", c.max(c.cond(c.eq("$operation", "OPE_REI"), "$total_avg", 0))),
					c.field("reint_proteo_avg", c.max(c.cond(c.eq("$operation", "reintContrato"), "$total_avg", 0)))),
				c.project(c.field("_id", false), c.rename("_id", "date"),
					c.field("ingr_sibis_execs", true), c.field("ingr_proteo_execs", true), c.field("reint_sibis_execs", true), c.field("reint_proteo_execs", true),
					c.field("ingr_sibis_avg", true), c.field("ingr_proteo_avg", true), c.field("reint_sibis_avg", true), c.field("reint_proteo_avg", true),
					c.field("total_sibis_execs", c.add("ingr_sibis_execs", "reint_sibis_execs")),
					c.field("total_proteo_execs", c.add("ingr_proteo_execs", "reint_proteo_execs")),
					c.field("total_execs", c.add("ingr_sibis_execs", "reint_sibis_execs","ingr_proteo_execs", "reint_proteo_execs")))
				)
        );
    }
}
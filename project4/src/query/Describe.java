package query;

import parser.AST_Describe;
import parser.ParseException;
import relop.Schema;
import global.Minibase;
import heap.HeapFile;
import heap.HeapScan;
import global.RID;
import global.SearchKey;
import relop.Tuple;
import relop.Predicate;
import query.IndexDesc;
import index.HashIndex;
import global.AttrType;

/**
/**
 * Execution plan for describing tables.
 * print out the column name and datatype for the columns in the table.
 */
class Describe implements Plan {
	
	protected String tableName;
	protected Schema schema; 
	

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {
  	tableName = tree.getFileName();
  	schema = QueryCheck.tableExists(tableName);

  } // public Describe(AST_Describe tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
		int fieldCount = schema.getCount();
		String colName = "Name";
		String colType = "Column";
		String colPad = String.format("%1$-" + 15 + "s",colName);
		System.out.println(String.format("%1$" + 16 + "s",tableName));
		System.out.println("  " + colPad + colType);
		System.out.println("----------------------------");
   	for(int i = 0; i < fieldCount; i++){
   		String fn = String.format("%1$-" + 15 + "s", schema.fieldName(i));
   		int fti = schema.fieldType(i);
   		String ft = String.format("%1$-" + 10 + "s", AttrType.toString(fti));
   		System.out.println("| " + fn + ft + "|");
   	}
   	System.out.println("----------------------------");

  } // public void execute()

} // class Describe implements Plan



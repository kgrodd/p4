package query;

import parser.AST_Update;
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
/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {
 /** Name of the table to create. */
  protected String tableName;
	protected Object[] values;
	protected String[] columns;
  protected Schema schema;
	protected Predicate [][] preds;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {
		// make sure the file doesn't already exist
    tableName = tree.getFileName();
		values = tree.getValues();
		columns = tree.getColumns();
		preds = tree.getPredicates();

    // get and validate the requested schema
  	schema = QueryCheck.tableExists(tableName);
		QueryCheck.insertValues(schema, values);
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println(values.length + " rows affected. (Not implemented)");

  } // public void execute()

} // class Update implements Plan

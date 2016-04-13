package query;

import parser.AST_Delete;
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


/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {
  protected String tableName;
	protected Predicate[][] preds;
  protected Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
		tableName = tree.getFileName();
		preds = tree.getPredicates();  	
		schema = QueryCheck.tableExists(tableName);
		QueryCheck.predicates(schema, preds);

  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Delete implements Plan

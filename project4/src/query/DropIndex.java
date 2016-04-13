package query;

import parser.AST_DropIndex;
import global.Minibase;
import index.HashIndex;
import query.QueryException;
import query.Catalog;
import relop.Schema;
import heap.HeapFile;
import heap.HeapScan;
import global.SearchKey;
import global.RID;
import relop.Tuple;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

	/** Name of the index to drop. */
  protected String indexName;
  
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index doesn't exist
   */
  public DropIndex(AST_DropIndex tree) throws QueryException {
  	indexName = tree.getFileName();
  	QueryCheck.indexExists(indexName);

  } // public DropIndex(AST_DropIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

		Minibase.SystemCatalog.dropIndex(indexName);
		
		new HashIndex(indexName).deleteFile();
    // print the output message
    System.out.println("Dropped index " + indexName);


  } // public void execute()

} // class DropIndex implements Plan

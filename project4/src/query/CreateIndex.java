package query;

import parser.AST_CreateIndex;
import global.Minibase;
import index.HashIndex;
import query.QueryException;
import query.Catalog;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  protected String fileName;
  protected String ixTable;
  protected String ixColumn;
  private HashIndex hash; 
  
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
  
    // make sure the file doesn't already exist
    fileName = tree.getFileName();
		ixTable = tree.getIxTable();
		ixColumn = tree.getIxColumn();

		
    QueryCheck.fileNotExists(fileName);
    
  } 

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
  
   	new HashIndex(fileName);
   	
    // add the schema to the catalog
    Minibase.SystemCatalog.createIndex(fileName, ixTable, ixColumn);

    // print the output message

    System.out.println("Index created bitch.");


  } // public void execute()

} // class CreateIndex implements Plan

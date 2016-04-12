package query;

import parser.AST_CreateIndex;
//import global.Minibase;
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
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
  
    // make sure the file doesn't already exist
    fileName = tree.getFileName();

    // get and validate the requested schema
    try {
      QueryCheck.indexExists(fileName);
      ixTable = tree.getIxTable();
      ixColumn = tree.getIxColumn();
      
    } catch (QueryException exc) {
      throw new QueryException(exc.getMessage());
    }
  } 

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
  
    // create the index
    new HashIndex(fileName);
    
    //to do, vaildate it exists
    Catalog catalog = new Catalog(true);

    // add the schema to the catalog
    catalog.createIndex(fileName, ixTable, ixColumn);

    // print the output message
    System.out.println("Index created.");


  } // public void execute()

} // class CreateIndex implements Plan

package query;

import parser.AST_CreateIndex;
import global.Minibase;
import index.HashIndex;
import query.QueryException;
import query.Catalog;
import relop.Schema;
import heap.HeapFile;
import heap.HeapScan;
import global.SearchKey;
import global.RID;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  protected String indexName;
  protected String ixTable;
  protected String ixColumn;
  private Schema schema;
  
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
  
    // make sure the file doesn't already exist
    indexName = tree.getFileName();
		ixTable = tree.getIxTable();
		ixColumn = tree.getIxColumn();

    QueryCheck.fileNotExists(indexName);
   	schema = QueryCheck.tableExists(ixTable);
   	QueryCheck.columnExists(schema, ixColumn);

  } 

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
  
   	HashIndex hi = new HashIndex(indexName);
   	HeapFile hf = new HeapFile(ixTable);
   	HeapScan hs = hf.openScan();
   	
   	byte [] b_array;
   	RID rid = null;
   	SearchKey sk;
   	
   // add the schema to the catalog
    Minibase.SystemCatalog.createIndex(indexName, ixTable, ixColumn);	
   	
   	while(hs.hasNext()){
   		b_array = hs.getNext(rid);
   		sk = new SearchKey(b_array, (short)b_array.length);
   		hi.insertEntry(sk, rid);
   	}
   	hs.close();
   	
   	//hi.printSummary();
   

    // print the output message

    System.out.println("Index created " + indexName);


  } // public void execute()

} // class CreateIndex implements Plan

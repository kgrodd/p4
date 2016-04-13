package query;

import parser.AST_Insert;
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
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {
  /** Name of the table to create. */
  protected String tableName;
	protected Object[] values;
  protected Schema schema;
	protected HeapFile hf;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
		// make sure the file doesn't already exist
    tableName = tree.getFileName();
		values = tree.getValues();

    // get and validate the requested schema and values
  	schema = QueryCheck.tableExists(tableName);
		QueryCheck.insertValues(schema, values);
  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
		hf = new HeapFile(tableName);

		Tuple t = new Tuple(schema, values);
		RID r = t.insertIntoFile(hf);		
		byte [] ba = t.getData();

		IndexDesc [] inds = Minibase.SystemCatalog.getIndexes(tableName);
	//	if(inds.length > 0)
	//	new HashIndex(inds[0].indexName).printSummary();

	 	for (IndexDesc ind : inds) {
		//	System.out.println("column name : " + ind.columnName);
      (new HashIndex(ind.indexName)).insertEntry(new SearchKey(new Tuple(schema, ba).getField(ind.columnName)), r);
    }

    // print the output message
    System.out.println(values.length + " rows affected.");
	//	if(inds.length > 0)
	//	new HashIndex(inds[0].indexName).printSummary();
		

  } // public void execute()

} // class Insert implements Plan

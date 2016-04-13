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
	protected Predicate[][] preds;
	protected int[] fldNos;
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
		fldNos = QueryCheck.updateFields(schema, columns);
		QueryCheck.updateValues(schema, fldNos, values);
		QueryCheck.predicates(schema, preds);
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
		HeapFile hf = new HeapFile(tableName);
   	HeapScan hs = hf.openScan();
   	byte [] b_array;
   	RID rid = new RID();
   	boolean flag = false;
		int ct = 0;

   	while(hs.hasNext()){
			flag = false;
   		b_array = hs.getNext(rid);
			for(int i = 0; i < preds.length; i++) {
				for (int j = 0; j < preds[i].length; j++) {
					flag |= preds[i][j].evaluate(new Tuple(schema, b_array));
				}
				if(!flag) {
					break;
				}
			}
			if(flag) {
				hf.updateRecord(rid, b_array);
				ct++;
			}
   	}

   	hs.close();
    // print the output message
    System.out.println(ct + " row updated.");

  } // public void execute()

} // class Update implements Plan

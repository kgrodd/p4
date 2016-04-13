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
  
   	HeapFile hf = new HeapFile(tableName);
   	HeapScan hs = hf.openScan();
   	
   	byte [] b_array;
   	RID rid = new RID();
   	Tuple tuple;
   	int count = 0;
   	boolean flag;
   	
   	while(hs.hasNext()){
   		b_array = hs.getNext(rid);
   		tuple = new Tuple(schema, b_array);
   		flag = false;
   		for(int i = 0; i < preds.length; i++){	
   			for(int j = 0; j < preds[i].length; j++){
   				flag = preds[i][j].evaluate(tuple);
   				if(flag){
   					break;
   				}
   			}
   			if(!flag){
   				break;
   			}
   		}
   		if(flag){
   			hf.deleteRecord(rid);
   			count++;
   		}
   	}
   	
   	hs.close();
   	
    System.out.println(count + " rows affected.");	

  } // public void execute()

} // class Delete implements Plan

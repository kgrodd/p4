package query;

import global.SortKey;
import relop.Selection;
import relop.SimpleJoin;
import relop.Projection;
import relop.FileScan;
import parser.AST_Select;
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
import relop.Iterator;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {
	protected String[] tables;
  protected Schema[] schemas;
	protected Schema combSchema;
	protected Predicate[][] preds;
	protected int[] fldNos;
	protected SortKey[] orderBy;
	protected Projection project;
	protected String[] columns;
	protected boolean isExp = false;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
			isExp = tree.isExplain;
			columns = tree.getColumns();
			orderBy = tree.getOrders();
			preds = tree.getPredicates();
			tables = tree.getTables();
			schemas = new Schema[tables.length];
			boolean selectFirst = false;
			SimpleJoin sj;
			Selection sel;
			Integer[] fldNo;

			for (int i = 0; i < tables.length; i++) {
				schemas[i] = QueryCheck.tableExists(tables[i]);
			}

			combSchema = schemas[0];

			for (int i = 1; i < tables.length; i++) {
				combSchema = combSchema.join(combSchema, schemas[1]);
			}

			for (int i = 0; i < tables.length; i++) {
				if((selectFirst = matchPred(i))) {
					Schema tempSc = schemas[i];
					String tempStr = tables[i];
					schemas[i] = schemas[0];
					tables[i] = tables[0];
					schemas[0] = tempSc;
					tables[0] = tempStr;
					break;
				}
			}
			if(columns.length != 0) {
				fldNos = QueryCheck.updateFields(combSchema, columns);
				fldNo	= new Integer [fldNos.length];
				
				for(int i = 0; i < fldNo.length; i++) 
					fldNo[i] = fldNos[i];
			} else {
				fldNo	= new Integer [combSchema.getCount()];

				for(int i = 0; i < fldNo.length; i++) 
					fldNo[i] = i;
			}



			QueryCheck.predicates(combSchema, preds);

			if(selectFirst) {
				sel = getSel(null);
				if(tables.length > 1) {
					sj = getFS(sel);
					project = new Projection(sj, fldNo);
				} else {
					project = new Projection(sel, fldNo);
				}
			} else {
				sj = getFS(null);
				sel = getSel(sj);
				project = new Projection(sel, fldNo);
			}

  } // public Select(AST_Select tree) throws QueryException

	public boolean matchPred(int ind) {
		try{
			QueryCheck.predicates(schemas[ind], preds);
			return true;
		} catch(QueryException e) {
			return false;
		}
	}

	public Selection getSel(SimpleJoin sj) {
		Selection sel;

		if(sj == null) {
			sel = new Selection(new FileScan(schemas[0], new HeapFile(tables[0])), preds[0]);
		} else {
			sel = new Selection(sj, preds[0]);
		}
		
		for(int i = 1; i < preds.length; i++) 
			sel = new Selection(sel, preds[i]);

		return sel;
	}

	public SimpleJoin getFS(Selection sel) {
		FileScan[] fsArr = new FileScan[tables.length];

		for (int i = 0; i < tables.length; i++) {
			fsArr[i] = new FileScan(schemas[i], new HeapFile(tables[i]));		  	
		}

		SimpleJoin sj;

		if(sel == null) {
			sj = new SimpleJoin(fsArr[0], fsArr[1]);
		} else {
			sj = new SimpleJoin(sel, fsArr[1]);
		}

		for (int i = 2; i < tables.length; i++) {
			sj = new SimpleJoin(sj, fsArr[i]); 
		} 

		return sj;
	}

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
		if(isExp) {
    	System.out.println("Explain Select");
			project.explain(0);
		} else {
			int ct = project.execute();
    	System.out.println("\n" + ct + " rows Selected.");
		}
		project.close();
  } // public void execute()

} // class Select implements Plan

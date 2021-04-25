#include "ParseTree.h"
#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "Statistics.h"
#include "Comparison.h"
#include <unordered_map>
#include "Pipe.h"
#include "RelOp.h"

extern "C" {
	int yyparse (void);   // defined in y.tab.c
}

using namespace std;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";

const int ncustomer = 150000;
const int nlineitem = 6001215;
const int nnation = 25;
const int norders = 1500000;
const int npart = 200000;
const int npartsupp = 800000;
const int nregion = 5;
const int nsupplier = 10000;

static int pidBuffer = 0;

extern int queryType;  

extern char *outputVar;
	
extern char *tableName;
extern char *fileToInsert;

extern struct AttrList *attsToCreate;
extern struct NameList *attsToSort;

char *catalog = "catalog";
char *stats = "Statistics.txt";

const int BUFFSIZE = 100;

unordered_map<int,Pipe *> pMap;

int getPid () {
	
	return ++pidBuffer;
	
}

enum NodeType {
	G, SF, SP, P, D, S, GB, J, W
};

// Parent class to hold Query objects
class QueryNode {

public:
	
	RelationalOp *relOp;
	int pid;
	NodeType t;
	Schema sch;

	QueryNode ();
	QueryNode (NodeType type) : t (type) {}
	virtual void Wait(){relOp->WaitUntilDone();}
	virtual void Execute(unordered_map<int, Pipe *> &pMap){}
	virtual void Print () {};
	~QueryNode () {}
};
// Class to hold Join info in the Query
class JoinNode : public QueryNode {

public:
	
	QueryNode *left;
	QueryNode *right;
	CNF cnf;
	Record literal;
	
	JoinNode () : QueryNode (J) {}
	~JoinNode () {
		
		if (left) delete left;
		if (right) delete right;
		
	}
	
	void Execute(unordered_map<int, Pipe *> &pMap){
		pMap[pid] = new Pipe(BUFFSIZE);
		relOp = new Join();

		left->Execute(pMap);
		right->Execute(pMap);
		((Join *)relOp)->Run(*(pMap[left->pid]),*(pMap[right->pid]),*(pMap[pid]),cnf,literal);

		left->Wait();right->Wait();

	}

	void Print () {
		left->Print ();
		right->Print ();
		cout << "*********************" << endl;
		cout << "Join Operation" << endl;
		cout << "Input Pipe 1 ID : " << left->pid << endl;
		cout << "Input Pipe 2 ID : " << right->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema : " << endl;
		sch.Print ();
		cout << "Join CNF : " << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
		
		
	}
	
};
// Class to hold Project info in the Query
class ProjectNode : public QueryNode {

public:
	
	int numIn;
	int numOut;
	int *attsToKeep;
	
	QueryNode *from;
	
	ProjectNode () : QueryNode (P) {}
	~ProjectNode () {
		
		if (attsToKeep) delete[] attsToKeep;
		
	}

	void Execute (unordered_map<int, Pipe *> &pMap) {
		pMap[pid] = new Pipe(BUFFSIZE);
		relOp = new Project();
		from->Execute(pMap);
		((Project *)relOp)->Run (*(pMap[from->pid]), *(pMap[pid]), attsToKeep, numIn, numOut);
		from->Wait();
	}
	
	void Print () {
		from->Print ();
		cout << "*********************" << endl;
		cout << "Project Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID " << pid << endl;
		cout << "Number Attrs Input : " << numIn << endl;
		cout << "Number Attrs Output : " << numOut << endl;
		cout << "Attrs To Keep :" << endl;
		for (int i = 0; i < numOut; i++) {
			
			cout << attsToKeep[i] << endl;
			
		}
		cout << "*********************" << endl;
		
		
		
	}
	
};
// Class to hold Select File info in the Query
class SelectFileNode : public QueryNode {

public:
	
	bool opened;
	
	CNF cnf;
	DBFile file;
	Record literal;
	
	SelectFileNode () : QueryNode (SF) {}
	~SelectFileNode () {
		
		if (opened) {
			
			file.Close ();
			
		}
		
	}

	void Execute (unordered_map<int, Pipe *> &pMap) {
		pMap[pid] = new Pipe(BUFFSIZE);
		relOp = new SelectFile();
		((SelectFile *)relOp)->Run(file,*(pMap[pid]),cnf,literal);
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Select File Operation" << endl;
		cout << "Output Pipe ID " << pid << endl;
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "Select CNF:" << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
	}
	
};

// Class to hold Select Pipe info in the Query
class SelectPipeNode : public QueryNode {

public:
	
	CNF cnf;
	Record literal;
	QueryNode *from;
	
	SelectPipeNode () : QueryNode (SP) {}
	~SelectPipeNode () {
		
		if (from) delete from;
		
	}

	void Execute (unordered_map<int, Pipe *> &pMap) {
		pMap[pid] = new Pipe(BUFFSIZE);
		relOp = new SelectPipe();
		((SelectPipe *)relOp)->Run(*(pMap[from->pid]),*(pMap[pid]),cnf,literal);
		from->Wait();
	}
	
	void Print () {
		from->Print ();
		cout << "*********************" << endl;
		cout << "Select Pipe Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "Select CNF:" << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
		
		
	}
	
};

// Class to hold Sum info in the Query
class SumNode : public QueryNode {

public:
	
	Function compute;
	QueryNode *from;
	
	SumNode () : QueryNode (S) {}
	~SumNode () {
		
		if (from) delete from;
		
	}

	void Execute (unordered_map<int, Pipe *> &pMap) {
		pMap[pid] = new Pipe(BUFFSIZE);
		relOp = new Sum();
		from->Execute(pMap);
		((Sum*)relOp)->Run(*(pMap[from->pid]),*(pMap[pid]),compute);
		from->Wait();
	}
	
	void Print () {
		from->Print ();
		cout << "*********************" << endl;
		cout << "Sum Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Function :" << endl;
		compute.Print ();
		cout << "*********************" << endl;
		
		
		
	}
	
};

// Class to hold Distinct info in the Query
class DistinctNode : public QueryNode {

public:
	
	QueryNode *from;
	
	DistinctNode () : QueryNode (D) {}
	~DistinctNode () {
		
		if (from) delete from;
		
	}

	void Execute (unordered_map<int, Pipe *> &pMap) {
		pMap[pid] = new Pipe(BUFFSIZE);
		relOp = new DuplicateRemoval();
		from->Execute(pMap);
		((DuplicateRemoval*)relOp)->Run(*(pMap[from->pid]),*(pMap[pid]),sch);
		from->Wait();
	}
	
	void Print () {
		from->Print ();
		cout << "*********************" << endl;
		cout << "Duplication Elimation Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "*********************" << endl;
		
		
		
	}
	
};

// Class to hold Group By info in the Query
class GroupByNode : public QueryNode {

public:
	
	QueryNode *from;
	
	Function compute;
	OrderMaker group;
	
	GroupByNode () : QueryNode (GB) {}
	~GroupByNode () {
		
		if (from) delete from;
		
	}
	
	void Execute (unordered_map<int, Pipe *> &pMap) {
		pMap[pid] = new Pipe(BUFFSIZE);
		relOp = new GroupBy();
		from->Execute(pMap);
		((GroupBy*)relOp)->Run(*(pMap[from->pid]),*(pMap[pid]),group,compute);
		from->Wait();
	}

	void Print () {
		from->Print ();
		cout << "*********************" << endl;
		cout << "Group By Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema : " << endl;
		sch.Print ();
		cout << "Function : " << endl;
		compute.Print ();
		cout << "OrderMaker : " << endl;
		group.Print ();
		cout << "*********************" << endl;
		
		
		
	}
	
};

// Class to hold Write Out info in the Query
class WriteOutNode : public QueryNode {

public:
	
	QueryNode *from;
	
	FILE *output;
	
	WriteOutNode () : QueryNode (W) {}
	~WriteOutNode () {
		
		if (from) delete from;
		
	}

	void Execute (unordered_map<int, Pipe *> &pMap) {
		relOp = new WriteOut();
		from->Execute(pMap);
		((WriteOut*)relOp)->Run(*(pMap[from->pid]),output,sch);
		from->Wait();
	}
	
	void Print () {
		from->Print ();
		cout << "*********************" << endl;
		cout << "Write Out Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "*********************" << endl;
		
		
		
	}
	
};

typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;


// Creates the schema object for all the tables and inserts the objects into the map
void initSchemaMap (SchemaMap &map) {
	
	// map[string(region)] = Schema ("catalog", region);
	// map[string(part)] = Schema ("catalog", part);
	// map[string(partsupp)] = Schema ("catalog", partsupp);
	// map[string(nation)] = Schema ("catalog", nation);
	// map[string(customer)] = Schema ("catalog", customer);
	// map[string(supplier)] = Schema ("catalog", supplier);
	// map[string(lineitem)] = Schema ("catalog", lineitem);
	// map[string(orders)] = Schema ("catalog", orders);
	ifstream inputfs(catalog);
	while(!inputfs.eof()){
		char s[100];
		inputfs.getline(s,100);
		if(strcmp("BEGIN",s)==0){
			inputfs.getline(s,100);
			map[string(s)] = Schema(catalog,s);
		}
	}
	inputfs.close();
}

// Initializes the Statistics objects by adding all the relations and appropriate attributes
void initStatistics (Statistics &s) {
	
	s.AddRel (region, nregion);
	s.AddRel (nation, nnation);
	s.AddRel (part, npart);
	s.AddRel (supplier, nsupplier);
	s.AddRel (partsupp, npartsupp);
	s.AddRel (customer, ncustomer);
	s.AddRel (orders, norders);
	s.AddRel (lineitem, nlineitem);

	// region
	s.AddAtt (region, "r_regionkey", nregion);
	s.AddAtt (region, "r_name", nregion);
	s.AddAtt (region, "r_comment", nregion);
	
	// nation
	s.AddAtt (nation, "n_nationkey",  nnation);
	s.AddAtt (nation, "n_name", nnation);
	s.AddAtt (nation, "n_regionkey", nregion);
	s.AddAtt (nation, "n_comment", nnation);
	
	// part
	s.AddAtt (part, "p_partkey", npart);
	s.AddAtt (part, "p_name", npart);
	s.AddAtt (part, "p_mfgr", npart);
	s.AddAtt (part, "p_brand", npart);
	s.AddAtt (part, "p_type", npart);
	s.AddAtt (part, "p_size", npart);
	s.AddAtt (part, "p_container", npart);
	s.AddAtt (part, "p_retailprice", npart);
	s.AddAtt (part, "p_comment", npart);
	
	// supplier
	s.AddAtt (supplier, "s_suppkey", nsupplier);
	s.AddAtt (supplier, "s_name", nsupplier);
	s.AddAtt (supplier, "s_address", nsupplier);
	s.AddAtt (supplier, "s_nationkey", nnation);
	s.AddAtt (supplier, "s_phone", nsupplier);
	s.AddAtt (supplier, "s_acctbal", nsupplier);
	s.AddAtt (supplier, "s_comment", nsupplier);
	
	// partsupp
	s.AddAtt (partsupp, "ps_partkey", npart);
	s.AddAtt (partsupp, "ps_suppkey", nsupplier);
	s.AddAtt (partsupp, "ps_availqty", npartsupp);
	s.AddAtt (partsupp, "ps_supplycost", npartsupp);
	s.AddAtt (partsupp, "ps_comment", npartsupp);
	
	// customer
	s.AddAtt (customer, "c_custkey", ncustomer);
	s.AddAtt (customer, "c_name", ncustomer);
	s.AddAtt (customer, "c_address", ncustomer);
	s.AddAtt (customer, "c_nationkey", nnation);
	s.AddAtt (customer, "c_phone", ncustomer);
	s.AddAtt (customer, "c_acctbal", ncustomer);
	s.AddAtt (customer, "c_mktsegment", 5);
	s.AddAtt (customer, "c_comment", ncustomer);
	
	// orders
	s.AddAtt (orders, "o_orderkey", norders);
	s.AddAtt (orders, "o_custkey", ncustomer);
	s.AddAtt (orders, "o_orderstatus", 3);
	s.AddAtt (orders, "o_totalprice", norders);
	s.AddAtt (orders, "o_orderdate", norders);
	s.AddAtt (orders, "o_orderpriority", 5);
	s.AddAtt (orders, "o_clerk", norders);
	s.AddAtt (orders, "o_shippriority", 1);
	s.AddAtt (orders, "o_comment", norders);
	
	// lineitem
	s.AddAtt (lineitem, "l_orderkey", norders);
	s.AddAtt (lineitem, "l_partkey", npart);
	s.AddAtt (lineitem, "l_suppkey", nsupplier);
	s.AddAtt (lineitem, "l_linenumber", nlineitem);
	s.AddAtt (lineitem, "l_quantity", nlineitem);
	s.AddAtt (lineitem, "l_extendedprice", nlineitem);
	s.AddAtt (lineitem, "l_discount", nlineitem);
	s.AddAtt (lineitem, "l_tax", nlineitem);
	s.AddAtt (lineitem, "l_returnflag", 3);
	s.AddAtt (lineitem, "l_linestatus", 2);
	s.AddAtt (lineitem, "l_shipdate", nlineitem);
	s.AddAtt (lineitem, "l_commitdate", nlineitem);
	s.AddAtt (lineitem, "l_receiptdate", nlineitem);
	s.AddAtt (lineitem, "l_shipinstruct", nlineitem);
	s.AddAtt (lineitem, "l_shipmode", 7);
	s.AddAtt (lineitem, "l_comment", nlineitem);
	
}


// This functions copies the table names and aliases
void CopyTablesNamesAndAliases (TableList *tableList, Statistics &s, vector<char *> &tableNames, AliaseMap &map)	{
	int cnt = 0;
	while (tableList) {
		
		s.CopyRel (tableList->tableName, tableList->aliasAs);
		
		map[tableList->aliasAs] = tableList->tableName;
		
		tableNames.push_back (tableList->aliasAs);
		
		tableList = tableList->next;
		cnt++;
	}
	// cout<<"CopyTablesNamesAndAliases cnt: "<<cnt<<endl;
	
}

// This functions copies the name list
void CopyNameList(NameList *nameList, vector<string> &names) {
	
	while (nameList) {
		
		names.push_back (string (nameList->name));
		
		nameList = nameList->next;
	
	}
	
}

void CopyAttrList (AttrList *attrList, vector<Attribute> &atts) {
	
	while (attrList) {
		Attribute att;
		att.name = attrList->name;
		switch (attrList->type) {
			case 0 :att.myType = Int;break;
			
			case 1 :att.myType = Double;break;
			
			case 2 : att.myType = String;break;

			default : {}
			
		}	
		atts.push_back (att);
		attrList = attrList->next;
	}
}

// Driver function to run A5
int main () {

	outputVar = "STDOUT";
	
	while (1) {
		
		cout << endl;
		
		yyparse ();
		
		if (queryType == 1) {
			
			// cout << "SELECT" << endl;
			/*
			cout << endl << "Print Boolean :" << endl;
			PrintParseTree (boolean);
			
			cout << endl << "Print TableList :" << endl;
			PrintTablesAliases (tables);
			
			cout << endl << "Print NameList groupingAtts :" << endl;
			PrintNameList (groupingAtts);
			
			cout << endl << "Print NameList attsToSelect:" << endl;
			PrintNameList (attsToSelect);
			
			cout << finalFunction << endl;
			cout << endl << "Print Function:" << endl;
			PrintFunction (finalFunction);
			
			cout << endl;
			*/
			vector<char *> tableNames;
			vector<char *> joinOrder;
			vector<char *> buffer (2);
			
			AliaseMap aliaseMap;
			SchemaMap schemaMap;
			Statistics s;
			
	//		cout << "!!!" << endl;
			initSchemaMap (schemaMap);
	//		initStatistics (s);
			s.Read (stats);
	//		cout << "!!!" << endl;
			CopyTablesNamesAndAliases (tables, s, tableNames, aliaseMap);
			
	//		cout << tableNames.size () << endl;
			
	/*		for (auto iter = tableNames.begin (); iter != tableNames.end (); iter++) {
				
				cout << *iter << endl;
				
			}*/
			
			if (tableNames.size () > 2) {
				
				sort (tableNames.begin (), tableNames.end ());
				
				int minCost = INT_MAX, cost = 0;
				int counter = 1;
				
				do {
					
					Statistics temp (s);
					
					auto iter = tableNames.begin ();
					buffer[0] = *iter;
					
			//		cout << *iter << " ";
					iter++;
					
					while (iter != tableNames.end ()) {
						
			//			cout << *iter << " ";
						buffer[1] = *iter;
						
						cost += temp.Estimate (boolean, &buffer[0], 2);
						temp.Apply (boolean, &buffer[0], 2);
						
						if (cost <= 0 || cost > minCost) {
							
							break;
							
						}
						
						iter++;
					
					}
					
			//		cout << endl << cost << endl;
			//		cout << counter++ << endl << endl;
					
					if (cost > 0 && cost < minCost) {
						
						minCost = cost;
						joinOrder = tableNames;
						
					}
					
			//		char fileName[10];
			//		sprintf (fileName, "t%d.txt", counter - 1);
			//		temp.Write (fileName);
					
					cost = 0;
					
				} while (next_permutation (tableNames.begin (), tableNames.end ()));
			
			} else {
				
				joinOrder = tableNames;
				
			}
		//	cout << minCost << endl;
			
			QueryNode *root;
			
			auto iter = joinOrder.begin ();
			SelectFileNode *selectFileNode = new SelectFileNode ();
			
			char filepath[50];
		//	cout << aliaseMap[*iter] << endl;
			sprintf (filepath, "bin/%s.bin", aliaseMap[*iter].c_str ());
			
			selectFileNode->file.Open (filepath);
			selectFileNode->opened = true;
			selectFileNode->pid = getPid ();
			selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
			selectFileNode->sch.Reseat (*iter);
			
			selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
			
			iter++;
			if (iter == joinOrder.end ()) {
				
				root = selectFileNode;
				
			} else {
				
				JoinNode *joinNode = new JoinNode ();
				
				joinNode->pid = getPid ();
				joinNode->left = selectFileNode;
				
				selectFileNode = new SelectFileNode ();
				
				sprintf (filepath, "bin/%s.bin", aliaseMap[*iter].c_str ());
				selectFileNode->file.Open (filepath);
				selectFileNode->opened = true;
				selectFileNode->pid = getPid ();
				selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
				
				selectFileNode->sch.Reseat (*iter);
				selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
				
				joinNode->right = selectFileNode;
				joinNode->sch.JoinSchema (joinNode->left->sch, joinNode->right->sch);
				joinNode->cnf.GrowFromParseTree (boolean, &(joinNode->left->sch), &(joinNode->right->sch), joinNode->literal);
				
				iter++;
				
				while (iter != joinOrder.end ()) {
					
					JoinNode *p = joinNode;
					
					selectFileNode = new SelectFileNode ();
					
					sprintf (filepath, "bin/%s.bin", (aliaseMap[*iter].c_str ()));
					selectFileNode->file.Open (filepath);
					selectFileNode->opened = true;
					selectFileNode->pid = getPid ();
					selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
					selectFileNode->sch.Reseat (*iter);
					selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
					
					joinNode = new JoinNode ();
					
					joinNode->pid = getPid ();
					joinNode->left = p;
					joinNode->right = selectFileNode;
					
					joinNode->sch.JoinSchema (joinNode->left->sch, joinNode->right->sch);
					joinNode->cnf.GrowFromParseTree (boolean, &(joinNode->left->sch), &(joinNode->right->sch), joinNode->literal);
					
					iter++;
					
				}
				
				root = joinNode;
				
			}
			
			QueryNode *temp = root;
			
			if (groupingAtts) {
				
				if (distinctFunc) {
					
					root = new DistinctNode ();
					
					root->pid = getPid ();
					root->sch = temp->sch;
					((DistinctNode *) root)->from = temp;
					
					temp = root;
					
				}
				
				root = new GroupByNode ();
				
				vector<string> groupAtts;
				CopyNameList (groupingAtts, groupAtts);
				
				root->pid = getPid ();
				((GroupByNode *) root)->compute.GrowFromParseTree (finalFunction, temp->sch);
				root->sch.GroupBySchema (temp->sch, ((GroupByNode *) root)->compute.ReturnInt ());
				((GroupByNode *) root)->group.growFromParseTree (groupingAtts, &(root->sch));
				
				((GroupByNode *) root)->from = temp;
				
			} else if (finalFunction) {
				
				root = new SumNode ();
				
				root->pid = getPid ();
				((SumNode *) root)->compute.GrowFromParseTree (finalFunction, temp->sch);
				
				Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
				root->sch = Schema (NULL, 1, ((SumNode *) root)->compute.ReturnInt () ? atts[0] : atts[1]);
				
				((SumNode *) root)->from = temp;
				
			} else if (attsToSelect) {
				
				root = new ProjectNode ();
				
				vector<int> attsToKeep;
				vector<string> atts;
				CopyNameList (attsToSelect, atts);
				
				// cout << atts.size () << endl;
				
				root->pid = getPid ();
				root->sch.ProjectSchema (temp->sch, atts, attsToKeep);
				
				int *attstk = new int[attsToKeep.size ()];
				
				for (int i = 0; i < attsToKeep.size (); i++) {
					
					attstk[i] = attsToKeep[i];
					
				}
				
				((ProjectNode *) root)->attsToKeep = attstk;
				((ProjectNode *) root)->numIn = temp->sch.GetNumAtts ();
				((ProjectNode *) root)->numOut = atts.size ();
				
				((ProjectNode *) root)->from = temp;
				
			}
			
			if (strcmp (outputVar, "NONE") && strcmp (outputVar, "STDOUT")) {
				
				temp = new WriteOutNode ();
				
				temp->pid = root->pid;
				temp->sch = root->sch;
				((WriteOutNode *)temp)->output = fopen (outputVar, "w");
				((WriteOutNode *)temp)->from = root;
				
				root = temp;
				
			}
			
			if (strcmp (outputVar, "NONE") == 0) {
				
				cout << "Parse Tree : " << endl;
				root->Print ();
			
			} else {
				
				root->Execute (pMap);
				
			}
			
			int i = 0;
			
			if (strcmp (outputVar, "STDOUT") == 0) {
				
				Pipe *p = pMap[root->pid];
				Record rec;
				
				while (p->Remove (&rec)) {
					
					i++;
					
					rec.Print (&(root->sch));
					
				}
				
			}
			
			cout << i << " records found!" << endl;
			
		} else if (queryType == 2) {
			
			if (attsToSort) {
				
				// PrintNameList (attsToSort);
				
			}
			
			char fileName[100];
			char tpchName[100];
			
			sprintf (fileName, "bin/%s.bin", tableName);
			sprintf (tpchName, "tpch/%s.tbl", tableName);
			
			DBFile file;
			
			vector<Attribute> attsCreate;
			
			CopyAttrList (attsToCreate, attsCreate);
			
			ofstream ofs(catalog, ifstream :: app);
			
			ofs << endl;
			ofs << "BEGIN" << endl;
			ofs << tableName << endl;
			ofs << tpchName <<endl;
			
			Statistics s;
			s.Read (stats);
//			s.Write (stats);
			s.AddRel (tableName, 0);
			
			for (auto iter = attsCreate.begin (); iter != attsCreate.end (); iter++) {
				
				s.AddAtt (tableName, iter->name, 0);
				
				ofs << iter->name << " ";
				
				cout << iter->myType << endl;
				switch (iter->myType) {
					
					case Int : {
						ofs << "Int" << endl;
					} break;
					
					case Double : {
						
						ofs << "Double" << endl;
						
					} break;
					
					case String : {
						
						ofs << "String" << endl;
						
					}
					default : {}
					
				}
				
			}
			
			ofs << "END" << endl;
			s.Write (stats);
			
			if (!attsToSort) {
				
				file.Create (fileName, heap, NULL);
			
			} else {
				
				Schema sch (catalog, tableName);
				
				OrderMaker order;
				
				order.growFromParseTree (attsToSort, &sch);
				
				SortInfo info;
				
				info.o = &order;
				info.runlen = BUFFSIZE;
				
				file.Create (fileName, sorted, &info);
				
			}
			
		} else if (queryType == 3) {
			
			char fileName[100];
			char metaName[100];
			char *tempFile = "tempfile.txt";
			
			sprintf (fileName, "bin/%s.bin", tableName);
			sprintf (metaName, "%s.md", fileName);
			
			remove (fileName);
			remove (metaName);
			
			ifstream ifs (catalog);
			ofstream ofs (tempFile);
			
			while (!ifs.eof ()) {
				
				char line[100];
				
				ifs.getline (line, 100);
				
				if (strcmp (line, "BEGIN") == 0) {
					
					ifs.getline (line, 100);
					
					if (strcmp (line, tableName)) {
						
						ofs << endl;
						ofs << "BEGIN" << endl;
						ofs << line << endl;
						
						ifs.getline (line, 100);
						
						while (strcmp (line, "END")) {
							
							ofs << line << endl;
							ifs.getline (line, 100);
							
						}
						
						ofs << "END" << endl;
						
					}
					
				}
				
			}
			
			ifs.close ();
			ofs.close ();
			
			remove (catalog);
			rename (tempFile, catalog);
			remove (tempFile);
			
		} else if (queryType == 4) {
			char fileName[100];
			char tpchName[100];
			
			sprintf (fileName, "bin/%s.bin", tableName);
			sprintf (tpchName, "tcph/%s", fileToInsert);
			DBFile file;
			Schema sch (catalog, tableName);
			
			sch.Print ();
			
			if (file.Open (fileName)) {
				file.Load (sch, tpchName);
				file.Close ();
			}
			
		} else if (queryType == 5) {
		} else if (queryType == 6) {
			break;
		}
	
	}
	
	return 0;
	
}
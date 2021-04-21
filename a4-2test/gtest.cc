#include "ParseTree.h"
#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <iostream>
#include <algorithm>
// #include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "Statistics.h"
#include "Comparison.h"
#include "gtest/gtest.h"
#include <pthread.h>
using namespace std;
// SELECT l.l_orderkey, s.s_suppkey, o.o_orderkey FROM lineitem AS l, supplier AS s, orders AS o WHERE (l.l_suppkey = s.s_suppkey) AND (l.l_orderkey = o.o_orderkey)
// SELECT SUM DISTINCT (s.s_acctbal) FROM lineitem AS l, supplier AS s, orders AS o WHERE (l.l_suppkey = s.s_suppkey) AND (l.l_orderkey = o.o_orderkey) GROUP BY s.s_suppkey

extern "C" {
	int yyparse (void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
	void init_lexical_parser (char *); // defined in lex.yy.c (from Lexer.l)
	void close_lexical_parser (); // defined in lex.yy.c
	void init_lexical_parser_func (char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
	void close_lexical_parser_func (); // defined in lex.yyfunc.c
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

vector<string> qorder;

static int pidBuffer = 0;
int getPid () {
	
	return ++pidBuffer;
	
}

enum NodeType {
	G, SF, SP, P, D, S, GB, J, W
};

class QueryNode {

public:
	
	int pid;  // Pipe ID
	
	NodeType t;
	Schema sch;  // Ouput Schema
	
	QueryNode ();
	QueryNode (NodeType type) : t (type) {}
	
	~QueryNode () {}
	virtual void Print () {};
	
};

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
	void Print () {
		left->Print ();
		right->Print ();
		qorder.push_back("Join");
		
	}
	
};

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
	
	void Print () {
		qorder.push_back("Project");
		from->Print ();
		
	}
	
};

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
	
	void Print () {
		qorder.push_back("Select_File");
	}
	
};

class SelectPipeNode : public QueryNode {

public:
	
	CNF cnf;
	Record literal;
	QueryNode *from;
	
	SelectPipeNode () : QueryNode (SP) {}
	~SelectPipeNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		qorder.push_back("Select_Pipe");
		from->Print ();
		
	}
	
};

class SumNode : public QueryNode {

public:
	
	Function compute;
	QueryNode *from;
	
	SumNode () : QueryNode (S) {}
	~SumNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		qorder.push_back("Sum");
		from->Print ();
		
	}
	
};

class DistinctNode : public QueryNode {

public:
	
	QueryNode *from;
	
	DistinctNode () : QueryNode (D) {}
	~DistinctNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		qorder.push_back("Duplicate_Elimination");
		from->Print ();
		
	}
	
};

class GroupByNode : public QueryNode {

public:
	
	QueryNode *from;
	
	Function compute;
	OrderMaker group;
	
	GroupByNode () : QueryNode (GB) {}
	~GroupByNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		qorder.push_back("Group_By");
		from->Print ();
		
	}
	
};

class WriteOutNode : public QueryNode {

public:
	
	QueryNode *from;
	
	FILE *output;
	
	WriteOutNode () : QueryNode (W) {}
	~WriteOutNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		qorder.push_back("Write_Out");
		from->Print ();
		
	}
	
};

typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;

void initSchemaMap (SchemaMap &map) {
	
	map[string(region)] = Schema ("catalog", region);
	map[string(part)] = Schema ("catalog", part);
	map[string(partsupp)] = Schema ("catalog", partsupp);
	map[string(nation)] = Schema ("catalog", nation);
	map[string(customer)] = Schema ("catalog", customer);
	map[string(supplier)] = Schema ("catalog", supplier);
	map[string(lineitem)] = Schema ("catalog", lineitem);
	map[string(orders)] = Schema ("catalog", orders);
	
}

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

void PrintParseTree (struct AndList *andPointer) {
  
	
}

void PrintTablesAliases (TableList * tableList)	{
	
	while (tableList) {
		
		tableList = tableList->next;
		
	}
	
}

void CopyTablesNamesAndAliases (TableList *tableList, Statistics &s, vector<char *> &tableNames, AliaseMap &map)	{
	int cnt = 0;
	while (tableList) {
		
		s.CopyRel (tableList->tableName, tableList->aliasAs);
		
		map[tableList->aliasAs] = tableList->tableName;
		
		tableNames.push_back (tableList->aliasAs);
		
		tableList = tableList->next;
		cnt++;
	}
	
}

void PrintNameList(NameList *nameList) {
	
	while (nameList) {
		
		nameList = nameList->next;
	
	}
	
}

void CopyNameList(NameList *nameList, vector<string> &names) {
	
	while (nameList) {
		
		names.push_back (string (nameList->name));
		
		nameList = nameList->next;
	
	}
	
}

void PrintFunction (FuncOperator *func) {
}



string func(char *input){
	init_lexical_parser(input);
	yyparse ();
	close_lexical_parser();
	vector<char *> tableNames;
	vector<char *> joinOrder;
	vector<char *> buffer (2);
	
	AliaseMap aliaseMap;
	SchemaMap schemaMap;
	Statistics s;
	
	initSchemaMap (schemaMap);
	initStatistics (s);
	CopyTablesNamesAndAliases (tables, s, tableNames, aliaseMap);
	sort (tableNames.begin (), tableNames.end ());
	double minCost = DBL_MAX, cost = 0;
	int counter = 1;
	
	do {
		
		Statistics temp (s);
		
		auto iter = tableNames.begin ();
		buffer[0] = *iter;
		
		iter++;
		
		while (iter != tableNames.end ()) {
			
			buffer[1] = *iter;
			
			cost += temp.Estimate (boolean, &buffer[0], 2);
			temp.Apply (boolean, &buffer[0], 2);
			
			if (cost <= 0 || cost > minCost) {
				
				break;
				
			}
			
			iter++;
		
		}
		
		if (cost > 0 && cost < minCost) {
			
			minCost = cost;
			
			
		}
		joinOrder = tableNames;
		cost = 0;
		
	} while (next_permutation (tableNames.begin (), tableNames.end ()));
	

	reverse(joinOrder.begin(),joinOrder.end());
	QueryNode *root;
	
	auto iter = joinOrder.begin ();
	SelectFileNode *selectFileNode = new SelectFileNode ();
	
	char filepath[50];
	
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
		char *t = NULL;
		root->sch = Schema (t, 1, (((SumNode *) root)->compute.ReturnInt () ? atts[0] : atts[1]));
		((SumNode *) root)->from = temp;
		
	} else if (attsToSelect) {
		root = new ProjectNode ();
		
		vector<int> attsToKeep;
		vector<string> atts;
		CopyNameList (attsToSelect, atts);
		
		root->pid = getPid ();
		root->sch.ProjectSchema (temp->sch, atts, attsToKeep);
		((ProjectNode *) root)->attsToKeep = &attsToKeep[0];
		((ProjectNode *) root)->numIn = temp->sch.GetNumAtts ();
		((ProjectNode *) root)->numOut = atts.size ();
		
		((ProjectNode *) root)->from = temp;
		
	}
	
	root->Print ();
	string res = qorder[0];
	for(int i=1;i<qorder.size();i++){
		res+="<-"+qorder[i];
	}
	qorder.clear();
	return res;
}



TEST (EXECUTIONPLAN,TC1){
	char *input="SELECT n.n_nationkey FROM nation AS n WHERE (n.n_name = 'UNITED STATES')";
	string res = func(input);
	cout<<"Query: "<<input<<endl;
	cout<<"Execution Plan: "<<res<<endl;
	ASSERT_EQ(res,"Project<-Select_File");
}
TEST (EXECUTIONPLAN,TC2){
	char *input="SELECT n.n_name FROM nation AS n, region AS r WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey > 5)";
	string res = func(input);
	cout<<"Query: "<<input<<endl;
	cout<<"Execution Plan: "<<res<<endl;
	ASSERT_EQ(res,"Project<-Select_File<-Select_File<-Join");
}
TEST (EXECUTIONPLAN,TC3){
	char *input="SELECT SUM (n.n_nationkey) FROM nation AS n, region AS r WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_name = 'UNITED STATES')";
	string res = func(input);
	cout<<"Query: "<<input<<endl;
	cout<<"Execution Plan: "<<res<<endl;
	ASSERT_EQ(res,"Sum<-Select_File<-Select_File<-Join");
}
TEST (EXECUTIONPLAN,TC4){
	char *input="SELECT SUM (n.n_regionkey) FROM nation AS n, region AS r WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_name = 'UNITED STATES') GROUP BY n.n_regionkey";
	string res = func(input);
	cout<<"Query: "<<input<<endl;
	cout<<"Execution Plan: "<<res<<endl;
	ASSERT_EQ(res,"Group_By<-Select_File<-Select_File<-Join");
}
TEST (EXECUTIONPLAN,TC5){
	char *input="SELECT SUM DISTINCT (n.n_nationkey + r.r_regionkey) FROM nation AS n, region AS r, customer AS c WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey = c.c_nationkey) AND (n.n_nationkey > 10) GROUP BY r.r_regionkey";
	string res = func(input);
	cout<<"Query: "<<input<<endl;
	cout<<"Execution Plan: "<<res<<endl;
	ASSERT_EQ(res,"Group_By<-Duplicate_Elimination<-Select_File<-Select_File<-Join<-Select_File<-Join");
}

/* Driver function to call all gtests*/
int main (int argc, char *argv[]) {
	
	testing::InitGoogleTest(&argc, argv);
	
	return RUN_ALL_TESTS ();
	
	
}
#include "gtest/gtest.h"
#include <pthread.h>
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;
// #pragma GCC diagnostic push
// #pragma GCC diagnostic ignored "-Wwrite-strings"


using namespace std;
char *fileName = "Statistics.txt";

int tc1 (){

	Statistics s;
        char *relName[] = {"lineitem"};

	s.AddRel(relName[0],6001215);
	s.AddAtt(relName[0], "l_returnflag",3);
	s.AddAtt(relName[0], "l_discount",11);
	s.AddAtt(relName[0], "l_shipmode",7);

		
	char *cnf = "(l_returnflag = 'R') AND (l_discount < 0.04 OR l_shipmode = 'MAIL')";

	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName, 1);
	return result;
}



int tc2 (){

	Statistics s;
        char *relName[] = {"orders","customer","nation"};

	
	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_custkey",150000);

	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);

	char *cnf = "(c_custkey = o_custkey)";
	yy_scan_string(cnf);
	yyparse();

	// Join the first two relations in relName
	s.Apply(final, relName, 2);
	
	cnf = " (c_nationkey = n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName, 3);
	return result;
}

int tc3 (){

	Statistics s;
        char *relName[] = { "customer", "orders", "lineitem"};

	s.AddRel(relName[0],150000);
	s.AddAtt(relName[0], "c_custkey",150000);
	s.AddAtt(relName[0], "c_mktsegment",5);

	s.AddRel(relName[1],1500000);
	s.AddAtt(relName[1], "o_orderkey",1500000);
	s.AddAtt(relName[1], "o_custkey",150000);
	
	s.AddRel(relName[2],6001215);
	s.AddAtt(relName[2], "l_orderkey",1500000);
	

	char *cnf = "(c_mktsegment = 'BUILDING')  AND (c_custkey = o_custkey)  AND (o_orderdate < '1995-03-1')";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);
	
	// cout<<"---------------------------"<<endl;
	cnf = " (l_orderkey = o_orderkey) ";
	yy_scan_string(cnf);
	yyparse();


	double result = s.Estimate(final, relName, 3);
	return result;
	

}
int tc4 (){

	Statistics s;
        char *relName[] = { "customer", "orders", "lineitem","nation"};

	//s.Read(fileName);
	
	s.AddRel(relName[0],150000);
	s.AddAtt(relName[0], "c_custkey",150000);
	s.AddAtt(relName[0], "c_nationkey",25);

	s.AddRel(relName[1],1500000);
	s.AddAtt(relName[1], "o_orderkey",1500000);
	s.AddAtt(relName[1], "o_custkey",150000);
	s.AddAtt(relName[1],"o_orderdate",-1);
	
	s.AddRel(relName[2],6001215);
	s.AddAtt(relName[2], "l_orderkey",1500000);
	
	s.AddRel(relName[3],25);
	s.AddAtt(relName[3], "n_nationkey",25);
	
	char *cnf = "(c_custkey = o_custkey)  AND (o_orderdate > '1994-01-23') ";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);

	cnf = " (l_orderkey = o_orderkey) ";
	yy_scan_string(cnf);                                                                               	yyparse();

	s.Apply(final, relName, 3);  
	
	cnf = "(c_nationkey = n_nationkey) ";
	yy_scan_string(cnf);                                                                               	yyparse();	
	
	double result = s.Estimate(final, relName, 4);
	return result;

}


TEST (STATISTICS,TestCase1){
    // double estimate=tc1();
	// cout<<estimate<<endl;
	ASSERT_EQ(857316,tc1());
}
TEST (STATISTICS,TestCase2){
    // double estimate=tc2();
	// cout<<estimate<<endl;
	ASSERT_EQ(1.5e+06,tc2());
}
TEST (STATISTICS,TestCase3){
    // double estimate=tc3();
	// cout<<estimate<<endl;
	ASSERT_EQ(400080,tc3());
}
TEST (STATISTICS,TestCase4){
    // int estimate=tc4();
	// cout<<estimate<<endl;
	ASSERT_EQ(2000404,tc4());
}


/* Driver function to call all gtests*/
int main (int argc, char *argv[]) {
	
	testing::InitGoogleTest(&argc, argv);
	
	return RUN_ALL_TESTS ();
	
}
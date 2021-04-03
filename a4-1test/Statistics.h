#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <bits/stdc++.h>
using namespace std;

class Attribute{
	public:
	int uniqueTuples;
	string attributeName;

	Attribute();
	Attribute(int num, string name);
	Attribute(const Attribute &copyMe);
	Attribute &operator = (const Attribute &copyMe);
	~Attribute();
};

class RelationOp{
	public:
	int totalTuples;
	bool isJoint;
	string relationName;

	map<string,string> relJoint;
	map<string,Attribute> attrMap;

	RelationOp();
	RelationOp(int num,string name);
	RelationOp(const RelationOp &copyMe);
	RelationOp &operator = (const RelationOp &copyMe);

	bool isRelationPresent (string name);
	~RelationOp();
};

class Statistics
{
	private:
	int GetRelationForOperand(Operand *op,char *relationName[],int numJoin,RelationOp &relationInfo);
	double OrOperand(OrList *orList,char *relationName[],int numJoin);
	double AndOperand(AndList *andList,char *relationName[],int numJoin);
	double CompOperand(ComparisonOp *compOp,char *relationName[],int numJoin);


	public:
	map<string,RelationOp> relMap;

	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	Statistics &operator= (Statistics &copyMe);
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	bool isRelationInMap(string name,RelationOp &rel);

};

#endif

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
	Attribute(Attribute &copyMe);
	Attribute &operator = (const Attribute &copyMe);
	~Attribute();
};

class Relation{
	public:
	int totalTuples;
	bool isJoint;
	string relationName;

	map<string,string> relJoint;
	map<string,Attribute> attrMap;

	Relation();
	Relation(int num,string name);
	Relation(Relation &copyMe);
	Relation &operator = (const Relation &copyMe);

	bool isRelationPresent (string name);
	~Relation();
};

class Statistics
{
	private:
	int GetRelationForOperand(Operand *op,char *relationName[],int numJoin,Relation &relationInfo);
	double OrOperand(OrList *orList,char *relationName[],int numJoin);
	double AndOperand(AndList *andList,char *relationName[],int numJoin);
	double CompOperand(ComparisonOp *compOp,char *relationName[],int numJoin);


	public:
	map<string,Relation> relMap;

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

	bool isRelationInMap(string name,Relation &rel);

};

#endif

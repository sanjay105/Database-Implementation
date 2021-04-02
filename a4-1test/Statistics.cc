#include "Statistics.h"

Attribute :: Attribute(){}

Attribute :: Attribute(int num, string name){
    uniqueTuples = num;
    attributeName = name;
}

Attribute :: Attribute(Attribute &copyMe){
    uniqueTuples = copyMe.uniqueTuples;
    attributeName = copyMe.attributeName;
}

Attribute &Attribute :: operator = (const Attribute &copyMe){
    uniqueTuples = copyMe.uniqueTuples;
    attributeName = copyMe.attributeName;
    return *this;
}

Attribute :: ~Attribute(){}

Relation :: Relation(){}

Relation :: Relation(int num,string name){
    totalTuples = num;
    relationName = name;
}

Relation :: Relation(Relation &copyMe){
    totalTuples = copyMe.totalTuples;
    relationName = copyMe.relationName;
}

Relation &Relation :: operator = (const Relation &copyMe){
    totalTuples = copyMe.totalTuples;
    relationName = copyMe.relationName;
    return *this;
}

bool Relation :: isRelationPresent (string name){
    if ( (name == relationName) || (relJoint.find(name)!=relJoint.end()) )return true;
    return false;
}

Relation :: ~Relation(){}


Statistics::Statistics(){}

Statistics::Statistics(Statistics &copyMe)
{
    relMap.insert(copyMe.relMap.begin(),copyMe.relMap.end());
}

Statistics &Statistics:: operator=(Statistics &copyMe)
{
    relMap.insert(copyMe.relMap.begin(),copyMe.relMap.end());
    return *this;
}

Statistics::~Statistics(){}

int Statistics :: GetRelationForOperand(Operand *op,char *relationName[],int numJoin,Relation &relationInfo){

    if(op == NULL || relationName == NULL)return -1;

    for(auto itr = relMap.begin();itr != relMap.end();itr++){
        if( itr->second.attrMap.find(op->value)!=itr->second.attrMap.end()){
            relationInfo = itr->second;
            return 0;
        }
    }
    return -1;

}

double Statistics :: OrOperand(OrList *orList,char *relationName[],int numJoin){
    if(orList == NULL)return 0;
    double l=0,r=0;

    l = CompOperand(orList->left,relationName,numJoin);
    int cnt = 1;

    OrList *t = orList->rightOr;
    char *attributeName = orList->left->left->value;

    while(t){
        if(strcmp(attributeName,t->left->left->value)==0)cnt++;
        t = t->rightOr;
    }

    if(cnt > 1)return cnt*l;

    r = OrOperand(orList->rightOr,relationName,numJoin);

    return 1 - (1-l) * (1-r);

}

double Statistics :: AndOperand(AndList *andList,char *relationName[],int numJoin){
    if (andList == NULL)return 0;
    // double l=0,r=0;
    // l = OrOperand(andList->left,relationName,numJoin);
    // r = AndOperand(andList->rightAnd,relationName,numJoin);
    // return l*r;
    return OrOperand(andList->left,relationName,numJoin) * AndOperand(andList->rightAnd,relationName,numJoin);
}

double Statistics :: CompOperand(ComparisonOp *compOp,char *relationName[],int numJoin){
    Relation lRel,rRel;
    double l = 0, r = 0;
    int code = compOp->code;
    int lRes = GetRelationForOperand(compOp->left,relationName,numJoin,lRel);
    int rRes = GetRelationForOperand(compOp->right,relationName,numJoin,rRel);

    if(compOp->right->code==NAME){
        if(rRes==-1){
            r = -1;
        }else{
            r = rRel.attrMap[compOp->right->value].uniqueTuples;
        }
    }else{
        r = -1;
    }

    if(compOp->left->code==NAME){
        if(lRes==-1){
            l = -1;
        }else{
            l = lRel.attrMap[compOp->left->value].uniqueTuples;
        }
    }else{
        l = -1;
    }

    if(code == LESS_THAN || code == GREATER_THAN){
        return 1.0/3.0;
    }else if(code == EQUALS){
        if(l>r){
            return 1/l;
        }else{
            return 1/r;
        }
    }

    return 0;
    
}


void Statistics::AddRel(char *relName, int numTuples)
{
    string relationName(relName);
    Relation newRel(numTuples,relationName);
    relMap[relationName] = newRel;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string relationName(relName),attributeName(attName);
    Attribute newAttribute(numDistincts,attributeName);
    relMap[relationName].attrMap[attributeName] = newAttribute;
}

void Statistics::CopyRel(char *oldName, char *newName)
{

}
	
void Statistics::Read(char *fromWhere)
{

}

void Statistics::Write(char *fromWhere)
{

}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{

}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{

}


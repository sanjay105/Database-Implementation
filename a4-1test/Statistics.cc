#include "Statistics.h"

Attribute :: Attribute(){}

Attribute :: Attribute(int num, string name){
    uniqueTuples = num;
    attributeName = name;
}

Attribute :: Attribute(const Attribute &copyMe){
    uniqueTuples = copyMe.uniqueTuples;
    attributeName = copyMe.attributeName;
}

Attribute &Attribute :: operator = (const Attribute &copyMe){
    uniqueTuples = copyMe.uniqueTuples;
    attributeName = copyMe.attributeName;
    return *this;
}

Attribute :: ~Attribute(){}

RelationOp :: RelationOp(){}

RelationOp :: RelationOp(int num,string name){
    totalTuples = num;
    relationName = name;
}

RelationOp :: RelationOp(const RelationOp &copyMe){
    totalTuples = copyMe.totalTuples;
    relationName = copyMe.relationName;
}

RelationOp &RelationOp :: operator = (const RelationOp &copyMe){
    totalTuples = copyMe.totalTuples;
    relationName = copyMe.relationName;
    return *this;
}

bool RelationOp :: isRelationPresent (string name){
    if ( (name == relationName) || (relJoint.find(name)!=relJoint.end()) )return true;
    return false;
}

RelationOp :: ~RelationOp(){}


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

int Statistics :: GetRelationForOperand(Operand *op,char *relationName[],int numJoin,RelationOp &relationInfo){

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
    RelationOp lRel,rRel;
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
    RelationOp newRel(numTuples,relationName);
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
    string newRelation(newName),oldRelation(oldName);
    relMap[newRelation] = relMap[oldRelation];
    relMap[newRelation].relationName = newRelation;
    map<string,Attribute> newAttrMap;
    for(auto itr = relMap[newRelation].attrMap.begin(); itr != relMap[newRelation].attrMap.end(); itr++){
        Attribute temp(itr->second);
        temp.attributeName = newRelation+"."+itr->first;
        newAttrMap[temp.attributeName] = temp;
    }
    relMap[newRelation].attrMap = newAttrMap;
}
	
void Statistics::Read(char *fromWhere)
{
    ifstream in(fromWhere);
    if(!in){
        cout<<"File Doesn't exsist"<<endl;
        exit(0);
    }
    relMap.clear();
    int totalTuples, uniqueTuples, numJoint, numRelations, numAttributes;
    string relationName, jointName, attributeName;

    in >> numRelations;
    for(int i=0;i<numRelations;i++){
        in >> relationName;
        in >> totalTuples;

        AddRel(&relationName[0],totalTuples);
        in >> relMap[relationName].isJoint;

        if(relMap[relationName].isJoint){
            in >> numJoint;
            for(int j=0;j<numJoint;j++){
                in >> jointName;
                relMap[relationName].relJoint[jointName] = jointName;
            }
        }

        in >> numAttributes;
        for(int j=0;j<numAttributes;j++){
            in >> attributeName;
            in >> uniqueTuples;

            AddAtt(&relationName[0],&attributeName[0],uniqueTuples);
        }


    }
}

void Statistics::Write(char *fromWhere)
{
    ofstream out(fromWhere);
    
    out << relMap.size()<<endl;

    for( auto itr = relMap.begin();itr != relMap.end();itr++){
        out << itr->second.relationName <<endl;
        out << itr->second.totalTuples <<endl;
        out << itr->second.isJoint <<endl;

        if(itr->second.isJoint){
            out << itr->second.relJoint.size() <<endl;
            for( auto itr1 = itr->second.relJoint.begin(); itr1 != itr->second.relJoint.end(); ++itr1){
                out << itr1->second <<endl;
            }
        }

        out << itr->second.attrMap.size() <<endl;
        for( auto itr1 = itr->second.attrMap.begin(); itr1 != itr->second.attrMap.end(); ++itr1){
            out << itr1->second.attributeName << endl;
            out << itr1->second.uniqueTuples << endl;
        }
    }

}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{

}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    return 800000;
}


#include "Statistics.h"

AttributeOp :: AttributeOp(){}

// Constructor to initialize AttributeOp with name and unique tuples
AttributeOp :: AttributeOp(int num, string name){
    uniqueTuples = num;
    attributeName = name;
}

// Copy Constructor to perform deep copy of the AttributeOp object
AttributeOp :: AttributeOp(const AttributeOp &copyMe){
    uniqueTuples = copyMe.uniqueTuples;
    attributeName = copyMe.attributeName;
}

// Oveloading equals to operand to perform deep copy of the AttributeOp object
AttributeOp &AttributeOp :: operator = (const AttributeOp &copyMe){
    uniqueTuples = copyMe.uniqueTuples;
    attributeName = copyMe.attributeName;
    return *this;
}

AttributeOp :: ~AttributeOp(){}

Relation :: Relation(){}

// Constructor to initialize relation with name and unique tuples
Relation :: Relation(int num,string name){
    totalTuples = num;
    relationName = name;
}

// Copy Constructor to perform deep copy of the relation object
Relation :: Relation(const Relation &copyMe){
    totalTuples = copyMe.totalTuples;
    relationName = copyMe.relationName;
    isJoint = copyMe.isJoint;
    relJoint.insert(copyMe.relJoint.begin(),copyMe.relJoint.end());
    attrMap.insert(copyMe.attrMap.begin(),copyMe.attrMap.end());

}

// Oveloading equals to operand to perform deep copy of the relation object
Relation &Relation :: operator = (const Relation &copyMe){
    totalTuples = copyMe.totalTuples;
    relationName = copyMe.relationName;
    isJoint = copyMe.isJoint;
    relJoint.insert(copyMe.relJoint.begin(),copyMe.relJoint.end());
    attrMap.insert(copyMe.attrMap.begin(),copyMe.attrMap.end());
    return *this;
}

// checks and returns if there exsists a given relation or not
bool Relation :: isRelationPresent (string name){
    if ( (name == relationName) || (relJoint.find(name)!=relJoint.end()) )return true;
    return false;
}

Relation :: ~Relation(){}


Statistics::Statistics(){}

// Copy Constructor to perform deep copy of the statistics object
Statistics::Statistics(Statistics &copyMe)
{
    relMap.insert(copyMe.relMap.begin(),copyMe.relMap.end());
}

// Oveloading equals to operand to perform deep copy of the statistics object
Statistics &Statistics:: operator=(Statistics &copyMe)
{
    relMap.insert(copyMe.relMap.begin(),copyMe.relMap.end());
    return *this;
}

Statistics::~Statistics(){}

// Returns 0 if there exsists a relation and copies the relation object to relationInfo else return -1
int Statistics :: GetRelationForOperand(Operand *op,char *relationName[],int numJoin,Relation &relationInfo){

    if(op == NULL || relationName == NULL)return -1;

    for(auto itr = relMap.begin();itr != relMap.end();itr++){
        if( itr->second.attrMap.find(op->value)!=itr->second.attrMap.end()){
            relationInfo = itr->second;
            //cout<<"GETRELATIONFOROPERAND: relation name "<<itr->first<<" s_suppkey "<<itr->second.attrMap["s_suppkey"].uniqueTuples<<endl;
            //cout<<"GETRELATIONFOROPERAND: relation name "<<itr->first<<" ps_suppkey "<<itr->second.attrMap["ps_suppkey"].uniqueTuples<<endl;
            //cout<<"GETRELATIONFOROPERAND: relation name "<<itr->first<<" s_suppkey1 "<<relationInfo.attrMap["s_suppkey"].uniqueTuples<<endl;
            //cout<<"GETRELATIONFOROPERAND: relation name "<<itr->first<<" ps_suppkey1 "<<relationInfo.attrMap["ps_suppkey"].uniqueTuples<<endl;
            return 0;
        }
    }
    return -1;

}

// calculates the selectivity of the given OrList
double Statistics :: OrOperand(OrList *orList,char *relationName[],int numJoin){
    if(orList == NULL)return 0;
    double l=0,r=0;
    
    l = CompOperand(orList->left,relationName,numJoin);
    int cnt = 1;
    // cout<<"OrOperand: leftvalue:"<<orList->left->left->value<<" rightvalue:"<<orList->left->right->value<<" l:"<<l<<endl;
    OrList *t = orList->rightOr;
    char *attributeName = orList->left->left->value;

    while(t){
        if(strcmp(attributeName,t->left->left->value)==0)cnt++;
        t = t->rightOr;
    }
    // cout<<"Count "<<cnt<<endl;
    if(cnt > 1)return cnt*l;

    r = OrOperand(orList->rightOr,relationName,numJoin);
    // cout <<"OROPERAND: left "<<l<<" right "<<r<<endl;
    return 1 - (1-l) * (1-r);

}

// calculates the selectivity of the given AndList
double Statistics :: AndOperand(AndList *andList,char *relationName[],int numJoin){
    if (andList == NULL)return 1;
    double l=0,r=0;
    l = OrOperand(andList->left,relationName,numJoin);
    r = AndOperand(andList->rightAnd,relationName,numJoin);
    // cout <<"ANDOPERAND: left "<<l<<" right "<<r<<endl;
    return l*r;
    // return OrOperand(andList->left,relationName,numJoin) * AndOperand(andList->rightAnd,relationName,numJoin);
}

// calculates the selectivity of the given Comparison operand
double Statistics :: CompOperand(ComparisonOp *compOp,char *relationName[],int numJoin){
    Relation lRel,rRel;
    double l = 0, r = 0;
    
    int lRes = GetRelationForOperand(compOp->left,relationName,numJoin,lRel);
    int rRes = GetRelationForOperand(compOp->right,relationName,numJoin,rRel);
    int code = compOp->code;
    // cout<<compOp->left->value<<" "<<compOp->code<<" "<<compOp->right->value<<endl;

    if(compOp->left->code==NAME){
        if(lRes==-1){
            l = 1;
        }else{
            //cout << "COMPOPERAND: Left relationname "<<lRel.relationName<<" compOp "<<compOp->left->value<<endl;
            //cout << "COMPOPERAND: Left1 "<<lRel.attrMap[string(compOp->left->value)].uniqueTuples<<endl;
            //cout << "COMPOPERAND: Left2 "<<lRel.attrMap[compOp->left->value].uniqueTuples<<endl;
            l = lRel.attrMap[string(compOp->left->value)].uniqueTuples;
        }
    }else{
        l = -1;
    }
    if(compOp->right->code==NAME){
        if(rRes==-1){
            r = 1;
        }else{
            //cout << "COMPOPERAND: Right relationname "<<rRel.relationName<<" compOp "<<compOp->right->value<<endl;
            //cout << "COMPOPERAND: Right1 "<<lRel.attrMap[string(compOp->right->value)].uniqueTuples<<endl;
            //cout << "COMPOPERAND: Right2 "<<lRel.attrMap[compOp->right->value].uniqueTuples<<endl;
            r = rRel.attrMap[string(compOp->right->value)].uniqueTuples;
        }
    }else{
        r = -1;
    }
    // cout <<"COMPOPERAND: Result left "<<l<<" right "<<r<<endl;
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

// Adds the relation to the current statistics object  
void Statistics::AddRel(char *relName, int numTuples)
{
    string relationName(relName);
    Relation newRel(numTuples,relationName);
    relMap[relationName] = newRel;
    //cout<<"ADDREL: relationname "<<relationName<<" relmap size "<<relMap.size()<<endl;
}

// Adds the AttributeOp to the given relation in the current statistics object
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string relationName(relName),attributeName(attName);
    if (numDistincts == -1){
        numDistincts = relMap[relationName].totalTuples;
    }
    AttributeOp newAttribute(numDistincts,attributeName);
    relMap[relationName].attrMap[attributeName] = newAttribute;
    //cout<<"ADDATT: added "<<attributeName<<" AttributeOp to "<<relationName<<" relation which has "<<newAttribute.uniqueTuples<<" tuples"<<endl;
}

// Makes a copy of the relation object with the new name
void Statistics::CopyRel(char *oldName, char *newName)
{
    string newRelation(newName),oldRelation(oldName);
    relMap[newRelation] = relMap[oldRelation];
    relMap[newRelation].relationName = newRelation;
    map<string,AttributeOp> newAttrMap;
    for(auto itr = relMap[newRelation].attrMap.begin(); itr != relMap[newRelation].attrMap.end(); itr++){
        AttributeOp temp(itr->second);
        temp.attributeName = newRelation+"."+itr->first;
        newAttrMap[temp.attributeName] = temp;
    }
    relMap[newRelation].attrMap = newAttrMap;
}

// Reads the data from the file and modifies the current statistics object 
void Statistics::Read(char *fromWhere)
{
    ifstream in(fromWhere);
    if(!in){
        //cout<<"File Doesn't exsist"<<endl;
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

// Writes the current statistics object into a file
void Statistics::Write(char *fromWhere)
{
    ofstream out(fromWhere);
    
    out << relMap.size()<<endl;
    //cout << relMap.size()<<endl;
    for( auto itr = relMap.begin();itr != relMap.end();itr++){
        out << itr->second.relationName <<endl;
        out << itr->second.totalTuples <<endl;
        out << itr->second.isJoint <<endl;
        //cout << itr->second.relationName <<endl;
        //cout << itr->second.totalTuples <<endl;
        //cout << itr->second.isJoint <<endl;
        if(itr->second.isJoint){
            out << itr->second.relJoint.size() <<endl;
            //cout << itr->second.relJoint.size() <<endl;
            for( auto itr1 = itr->second.relJoint.begin(); itr1 != itr->second.relJoint.end(); ++itr1){
                out << itr1->second <<endl;
                //cout << itr1->second <<endl;
            }
        }

        out << itr->second.attrMap.size() <<endl;
        //cout << itr->second.attrMap.size() <<endl;
        for( auto itr1 = itr->second.attrMap.begin(); itr1 != itr->second.attrMap.end(); ++itr1){
            out << itr1->second.attributeName << endl;
            //cout << itr1->second.attributeName << endl;
            out << itr1->second.uniqueTuples << endl;
            //cout << itr1->second.uniqueTuples << endl;
        }
    }

}

// Modifies the statistics object after applying the given cnf
void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    int ind = 0, numJoin = 0;
	char *names[100];
	
	Relation rel;
	
	while (ind < numToJoin) {
		string buffer (relNames[ind]);
		auto iter = relMap.find (buffer);
		if (iter != relMap.end ()) {
			rel = iter->second;
			names[numJoin++] = relNames[ind];
			if (rel.isJoint) {
				int size = rel.relJoint.size();
				if (size <= numToJoin) {
					for (int i = 0; i < numToJoin; i++) {
						string buf (relNames[i]);
						if (rel.relJoint.find (buf) == rel.relJoint.end () &&
							rel.relJoint[buf] != rel.relJoint[buffer]) {
							return;
						}
					}
				}
			}
            else {	
				ind++;
				continue;
			}
			
		} 
		ind++;
		
	}
	
	double estimation = Estimate (parseTree, names, numJoin);
	ind = 1;
	string firstRelName (names[0]);
	Relation firstRel = relMap[firstRelName];
	Relation temp;
	relMap.erase (firstRelName);
	firstRel.isJoint = true;
	firstRel.totalTuples = estimation;
	// cout << "APPLY: "<<firstRelName<<" "<<firstRel.totalTuples<<endl;
//	//cout << firstRelName << endl;
//	//cout << estimation << endl;
	
	while (ind < numJoin) {
		string buffer (names[ind]);
		firstRel.relJoint[buffer] = buffer;
		temp = relMap[buffer];
		relMap.erase (buffer);
		firstRel.attrMap.insert (temp.attrMap.begin (), temp.attrMap.end ());
		ind++;
	}
	// relMap.insert ({firstRelName, firstRel});
    //cout<<"APPLY: relmapsize before"<<relMap.size()<<endl;
    relMap[firstRelName] = firstRel;
    //cout<<"APPLY: relmapsize after"<<relMap.size()<<endl;
}

// Estimates the result record count after applying given cnf
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    int ind = 0;
	double factor = 1.0, product = 1.0;
	while (ind < numToJoin) {
		string buffer (relNames[ind]);
		if (relMap.find (buffer) != relMap.end ()) {
			product *= (double) relMap[buffer].totalTuples;
		}
		ind++;
	}
	if (parseTree == NULL) return product;
	factor = AndOperand (parseTree, relNames, numToJoin);
	// cout <<"Product "<< product << endl;
	// cout <<"Factor" << factor << endl;
	// cout << product*factor <<endl;
	return factor * product;
}


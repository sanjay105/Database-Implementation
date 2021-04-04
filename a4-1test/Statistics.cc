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

Relation :: Relation(){}

Relation :: Relation(int num,string name){
    totalTuples = num;
    relationName = name;
}

Relation :: Relation(const Relation &copyMe){
    totalTuples = copyMe.totalTuples;
    relationName = copyMe.relationName;
    isJoint = copyMe.isJoint;
    relJoint.insert(copyMe.relJoint.begin(),copyMe.relJoint.end());
    attrMap.insert(copyMe.attrMap.begin(),copyMe.attrMap.end());

}

Relation &Relation :: operator = (const Relation &copyMe){
    totalTuples = copyMe.totalTuples;
    relationName = copyMe.relationName;
    isJoint = copyMe.isJoint;
    relJoint.insert(copyMe.relJoint.begin(),copyMe.relJoint.end());
    attrMap.insert(copyMe.attrMap.begin(),copyMe.attrMap.end());
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
            //cout<<"GETRELATIONFOROPERAND: relation name "<<itr->first<<" s_suppkey "<<itr->second.attrMap["s_suppkey"].uniqueTuples<<endl;
            //cout<<"GETRELATIONFOROPERAND: relation name "<<itr->first<<" ps_suppkey "<<itr->second.attrMap["ps_suppkey"].uniqueTuples<<endl;
            //cout<<"GETRELATIONFOROPERAND: relation name "<<itr->first<<" s_suppkey1 "<<relationInfo.attrMap["s_suppkey"].uniqueTuples<<endl;
            //cout<<"GETRELATIONFOROPERAND: relation name "<<itr->first<<" ps_suppkey1 "<<relationInfo.attrMap["ps_suppkey"].uniqueTuples<<endl;
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
    // cout<<"OrOperand: leftvalue:"<<orList->left->left->value<<" rightvalue:"<<orList->left->right->value<<" l:"<<l<<endl;
    OrList *t = orList->rightOr;
    char *attributeName = orList->left->left->value;

    while(t){
        if(strcmp(attributeName,t->left->left->value)==0)cnt++;
        t = t->rightOr;
    }

    if(cnt > 1)return cnt*l;

    r = OrOperand(orList->rightOr,relationName,numJoin);
    // cout <<"OROPERAND: left "<<l<<" right "<<r<<endl;
    return 1 - (1-l) * (1-r);

}

double Statistics :: AndOperand(AndList *andList,char *relationName[],int numJoin){
    if (andList == NULL)return 1;
    double l=0,r=0;
    l = OrOperand(andList->left,relationName,numJoin);
    r = AndOperand(andList->rightAnd,relationName,numJoin);
    // cout <<"ANDOPERAND: left "<<l<<" right "<<r<<endl;
    return l*r;
    // return OrOperand(andList->left,relationName,numJoin) * AndOperand(andList->rightAnd,relationName,numJoin);
}

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


void Statistics::AddRel(char *relName, int numTuples)
{
    string relationName(relName);
    Relation newRel(numTuples,relationName);
    relMap[relationName] = newRel;
    //cout<<"ADDREL: relationname "<<relationName<<" relmap size "<<relMap.size()<<endl;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string relationName(relName),attributeName(attName);
    if (numDistincts == -1){
        numDistincts = relMap[relationName].totalTuples;
    }
    Attribute newAttribute(numDistincts,attributeName);
    relMap[relationName].attrMap[attributeName] = newAttribute;
    //cout<<"ADDATT: added "<<attributeName<<" attribute to "<<relationName<<" relation which has "<<newAttribute.uniqueTuples<<" tuples"<<endl;
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

void Statistics::Write(char *fromWhere)
{
    ofstream out(fromWhere);
    
    out << relMap.size()<<endl;
    //cout << relMap.size()<<endl;
    for( auto itr = relMap.begin();itr != relMap.end();itr++){
        out << itr->second.relationName <<endl;
        out << (int)itr->second.totalTuples <<endl;
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
            out << (int)itr1->second.uniqueTuples << endl;
            //cout << itr1->second.uniqueTuples << endl;
        }
    }

}

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
							
							//cout << "Cannot be joined!" << endl;
							
							return;
							
						}
						
					}
					
				} else {
					
					//cout << "Cannot be joined!" << endl;
					
				}
			
			} else {
				
				ind++;
				
				continue;
				
			}
			
		} else {
			
			// //cout << buffer << " Not Found!" << endl;
			
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

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    int ind = 0;
	
	double factor = 1.0, product = 1.0;
	
	while (ind < numToJoin) {
		
		string buffer (relNames[ind]);
		
		if (relMap.find (buffer) != relMap.end ()) {
			
			//cout << buffer << " Found in Estimate!" << endl;
			product *= (double) relMap[buffer].totalTuples;
			
		} else {
			
			//cout << buffer << " Not Found!" << endl;
			
		}
		
		ind++;
	
	}
	
	if (parseTree == NULL) {
		
		return product;
		
	}
	
	factor = AndOperand (parseTree, relNames, numToJoin);
	
	// cout <<"Product "<< product << endl;
	// cout <<"Factor" << factor << endl;
	// cout << product*factor <<endl;
	return factor * product;
}


#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <bits/stdc++.h>
#include <string.h>
using namespace std;


HeapDBFile :: HeapDBFile(){
    curFile = new File();
    tempPage = new Page();
    isFileCreated = false;
    isFileOpened = false;
}

HeapDBFile :: ~HeapDBFile(){
    delete curFile;
    delete tempPage;
}

int HeapDBFile :: Create(const char *f_path){
    tempPage->EmptyItOut();
    curFile->Open(0,(char* )f_path); //ytd
    isFileCreated = true;
    pIndex = 0;
}

void HeapDBFile :: Load (Schema &f_schema, const char *loadpath) {
    FILE *loadFile = fopen(loadpath,"r");

    if (loadFile == NULL){
        cout<<loadpath<<" File doesn't exsist"<<endl;
        exit(0);
    }
    
    pIndex = 0;
    tempPage -> EmptyItOut();
    Record temp;
    
    // Iterates over the table and appends each record to the binary file
    while(temp.SuckNextRecord(&f_schema,loadFile)==1){
        Add(temp);
    }
    
    curFile -> AddPage(tempPage,pIndex++);
    tempPage -> EmptyItOut();
    fclose(loadFile);
    isFileOpened = true;
}

int HeapDBFile::Open (const char *f_path) {
    cout<<f_path<<endl;
    pIndex = 0;
    curFile->Open(1,(char* )f_path);
    tempPage -> EmptyItOut();
    isFileOpened = true;
    return 1;
}

void HeapDBFile::MoveFirst () {
    // cout<<"Start of MoveFirst Function"<<endl;

    pIndex = 0;
    tempPage -> EmptyItOut();
    curFile -> GetPage(tempPage,pIndex);

    // cout<<"End of MoveFirst Function"<<endl;
}

int HeapDBFile::Close () {
    // cout<<"Start of Close Function"<<endl;

    if(isFileOpened && tempPage -> GetRecordsCnt() > 0){
        isFileOpened = false;
        curFile -> AddPage(tempPage, pIndex);
        tempPage -> EmptyItOut();
    }
    if(!isFileOpened){
        return 0;
    }

    curFile->Close();
    
    // cout<<"End of Close Function"<<endl;
    return 1;
}

void HeapDBFile::Add (Record &rec) {
    // cout<<"Start of Add Function"<<endl;

    if(!tempPage -> Append(&rec)){
        curFile->AddPage(tempPage,pIndex++);
        tempPage ->EmptyItOut();
        tempPage ->Append(&rec);
    }

    // cout<<"End of Add Function"<<endl;
}

int HeapDBFile::GetNext (Record &fetchme) {
    // cout<<"Start of GetNext Function"<<endl;

    if (tempPage->GetFirst(&fetchme) == 0){

        if(++pIndex < curFile->GetLength()-1){

            curFile->GetPage(tempPage,pIndex);
            tempPage->GetFirst(&fetchme);
            // cout<<"End of GetNext Function"<<endl;

            return 1;

        }else{

            // cout<<"End of GetNext Function"<<endl;
            return 0;

        }
    }else{

        // cout<<"End of GetNext Function"<<endl;
        return 1;

    }

}

int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    // cout<<"Start of GetNext_ Function"<<endl;
    ComparisonEngine cEngine;
    while(GetNext(fetchme)==1){
        if(cEngine.Compare(&fetchme,&literal,&cnf)){
            // cout<<"End of GetNext_ Function"<<endl;
            return 1;

        }
    }
    // cout<<"End of GetNext_ Function"<<endl;
    return 0;
}


SortedDBFile :: SortedDBFile(OrderMaker *o,int runlen){
    this->bufSize = 100;
    this->runlen = runlen;
    this->o = o;
    this->query = NULL;
    this->bq = NULL;
    this->tempPage = new Page();
    this->curFile = new File();
}

SortedDBFile :: ~SortedDBFile(){
    delete query;
    delete tempPage;
    delete bq;
    delete curFile;
    delete o;
}

int SortedDBFile :: Create(const char *f_path){
    this->fpath = new char[100];
    strcpy(this->fpath, fpath);
    tempPage->EmptyItOut();
    writeMode = false;
    pIndex = 0;
    curFile->Open(0,this->fpath);
    return 1;
}

void SortedDBFile :: Load (Schema &f_schema, const char *loadpath) {
    Record tempRecord;
    FILE *tFile = fopen(loadpath,"r");
    if(tFile == NULL){
        cerr << "Cannot open the file"<<endl;
        exit(0);
    }
    pIndex = 0;
    tempPage->EmptyItOut();
    while(tempRecord.SuckNextRecord(&f_schema,tFile)){
        Add(tempRecord);
    }
    fclose(tFile);
}

int SortedDBFile::Open (const char *f_path) {
    this->fpath = new char[100];
    strcpy(this->fpath, fpath);
    tempPage->EmptyItOut();
    writeMode = false;
    pIndex = 0;
    curFile->Open(1,this->fpath);
    if(curFile->GetLength()>0){
        curFile->GetPage(tempPage,pIndex);
    }
    return 1;
}

void SortedDBFile::MoveFirst () {
    if (writeMode){
        MergeBigQToFile();
    }else{
        tempPage->EmptyItOut();
        pIndex = 0;
        if (curFile->GetLength()>0){
            curFile->GetPage(tempPage,pIndex);
        }
        if(query){
            delete query;
        }
    }
}

int SortedDBFile::Close () {
    if(writeMode){
        MergeBigQToFile();
    }
    curFile->Close();
    return 1;
}

void SortedDBFile::Add (Record &rec) {
    if(!writeMode){
        InitializeBigQ ();
    }
    inpipe->Insert(&rec);
}

int SortedDBFile::GetNext (Record &fetchRec) {
    if(writeMode){
        MergeBigQToFile();
    }
    if(tempPage->GetFirst(&fetchRec)){
        return 1;
    }else{
        pIndex++;
        if(pIndex<curFile->GetLength()-1){
            curFile->GetPage(tempPage,pIndex);
            tempPage->GetFirst(&fetchRec);
            return 1;
        }else{
            return 0;
        }
    }
    return 0;

}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if(writeMode){
        MergeBigQToFile();
    }
    ComparisonEngine compEng;
    if(!query){
        if (QueryOrderGenerator (*query, *o, cnf) > 0) {
			// query generated successfully
			// cout << "Query Gen Success! Go Bin Search!" << endl;
			query->Print ();
			if (BinarySearch (fetchme, cnf, literal)) {
				// cout << "Found!" << endl;
				// Found
				return 1;
				
			} else {
				// binary search fails
				// cout << "Not Found!" << endl;
				return 0;
				
			}
			
		} else {
			//query generated but is empty
			// cout << "Query Gen fail! Go Sequential!" << endl;
			return GetNextSequential (fetchme, cnf, literal);
			
		}
    }else{
        // query exists
		if (query->AttributeCount() == 0) {
			// invalid query
			return GetNextSequential (fetchme, cnf, literal);
			
		} else {
			// valid query
			return GetNextWithQuery (fetchme, cnf, literal);
			
		}
    }
    return 0;
}

void SortedDBFile :: MergeBigQToFile(){
    writeMode = false;
    inpipe->ShutDown();
    if(curFile->GetLength()>0){
        MoveFirst();
    }

    Record *fromFile = new Record;
    HeapDBFile *newHeap = new HeapDBFile;
    Record *fromPipe = new Record;
    ComparisonEngine compEng;

    newHeap->Create("bin/temp.bin");
    int flagFile = GetNext(*fromFile);
    int flagPipe = outpipe->Remove(fromPipe);

    while(flagFile&&flagPipe){
        if(compEng.Compare(fromPipe,fromFile,o)> 0){
            newHeap->Add(*fromFile);
            flagFile = GetNext(*fromFile);
        }else{
            newHeap->Add(*fromPipe);
            flagPipe = outpipe->Remove(fromPipe);
        }
    }
    while(flagFile){
        newHeap->Add(*fromFile);
        flagFile = GetNext(*fromFile);
    }
    while(flagPipe){
        newHeap->Add(*fromPipe);
        flagPipe = outpipe->Remove(fromPipe);
    }
    newHeap->Close();
    outpipe->ShutDown();
    curFile->Close();
    remove(fpath);
    rename("bin/temp.bin",fpath);
    curFile->Open(1,fpath);
    MoveFirst();
    delete newHeap;
    
}

int SortedDBFile :: QueryOrderGenerator (OrderMaker &query, OrderMaker &order, CNF &cnf){
    query.SetAttributeCount(0);
	bool gotIt = false;
	
	for (int i = 0; i < order.AttributeCount(); ++i) {
		for (int j = 0; j < cnf.GetNumAnds(); ++j) {
			if ((cnf.GetorLens(j) != 1 || cnf.orList[j][0].op != Equals)||
               (cnf.orList[i][0].operand1 == Left && cnf.orList[i][0].operand2 == Left) ||
               (cnf.orList[i][0].operand2 == Right && cnf.orList[i][0].operand1 == Right) ||
               (cnf.orList[i][0].operand1==Left && cnf.orList[i][0].operand2 == Right) ||
               (cnf.orList[i][0].operand1==Right && cnf.orList[i][0].operand2 == Left)) {
                continue;
			}
			if (cnf.orList[j][0].operand1 == Left && cnf.orList[j][0].whichAtt1 == order.whichAtts[i]) {
				query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt2;
				query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
				query.numAtts++;
				gotIt = true;
				break;
			}
			if (cnf.orList[j][0].operand2 == Left && cnf.orList[j][0].whichAtt2 == order.whichAtts[i]) {
				query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt1;
				query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
				query.SetAttributeCount(query.AttributeCount()+1);	
				gotIt = true;
				break;
			}
        }
		if (!gotIt) {
			break;
		}
	}
	return query.AttributeCount();
}

int SortedDBFile :: BinarySearch(Record &fetchme, CNF &cnf, Record &literal){
    off_t first = pIndex;
	off_t last = curFile->GetLength () - 1;
	off_t mid = pIndex;
	
	Page *page = new Page;
	ComparisonEngine comp;

	while (true) {
		mid = (first + last) / 2;
		curFile->GetPage (page, mid);
		if (page->GetFirst (&fetchme)) {
			if (comp.Compare (&literal, query, &fetchme, o) <= 0) {
				last = mid - 1;
				if (last <= first) break;
			} else {
				first = mid + 1;
				if (last <= first) break;
			}
		} else {
			break;
		}
	}
	if (comp.Compare (&fetchme, &literal, &cnf)) {
		delete tempPage;
		pIndex = mid;
		tempPage = page;
		return 1;
	} else {
		delete page;
		return 0;
	}
}

int SortedDBFile :: GetNextSequential (Record &fetchme, CNF &cnf, Record &literal){
    ComparisonEngine compEng;
	while (GetNext (fetchme)) {
		if (compEng.Compare (&fetchme, &literal, &cnf)){
			return 1;
		}
	}
	return 0;
}

int SortedDBFile :: GetNextWithQuery (Record &fetchme, CNF &cnf, Record &literal){
    ComparisonEngine compEng;
	while (GetNext (fetchme)) {
		if (!compEng.Compare (&literal, query, &fetchme, o)){
			if (compEng.Compare (&fetchme, &literal, &cnf)){
				return 1;
			}
		} else {
			break;
		}
	}
	return 0;
}

void SortedDBFile :: InitializeBigQ (){
    writeMode = true;
    inpipe = new Pipe(bufSize);
    outpipe = new Pipe(bufSize);

    bq = new BigQ(*inpipe,*outpipe,*o,runlen);
}

// Constructor: initializes File and Page objects
DBFile::DBFile () {
}

// Destructor: deletes File and Page objects
DBFile::~DBFile(){
    delete curDBObject;
}

// Creates a binary file at f_path of necessary file type 
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    // cout<<"Start of Create Function"<<endl;
    ofstream fs;
    string fname = f_path;
    fs.open(fname+".tmp");

    if (f_type == heap){
        fs << "heap" << endl;
        curDBObject = new HeapDBFile;
        // tempPage->EmptyItOut();
        // curFile->Open(0,(char* )f_path); //ytd
        // isFileCreated = true;
        // fileType = f_type;
        // pIndex = 0;
        

    }else if(f_type = sorted){
        
        fs<< "sorted" << endl;
        SortInfo *sinfo = (SortInfo*)startup;
        fs << sinfo->runlen << endl;
        fs << sinfo->o->AttributeCount() <<endl;
        sinfo->o->PrintOrderMakerToStream(fs);
        curDBObject = new SortedDBFile(sinfo->o,sinfo->runlen);
    }
    else{
        fs.close();
        // We can implement sorted and tree file type here
        cerr << "BAD. Invalid File type"<<endl;
        exit(2);
    }
    curDBObject->Create(f_path);
    fs.close();
    // cout<<"End of Create Function"<<endl;
    return 1;
}


// Loads the specifed DB table with its respective schema into a binary file
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    curDBObject->Load(f_schema,loadpath);
}

// Opens the binary file with the specified path and sets the pointer to first record
int DBFile::Open (const char *f_path) {
    ifstream fs;
    string fname = f_path;
    fs.open(fname+".tmp");
    if(fs.is_open()){
        string ftype;
        fs >> ftype;
        if(ftype == "heap"){
            curDBObject = new HeapDBFile;
        }else if (ftype == "sorted"){
            int runlen,numAtts;
            OrderMaker *order;
            fs >> runlen;
            fs >> numAtts;
            order->SetAttributeCount(numAtts);
            for( int i=0 ;i < numAtts; i++){
                int attributeNum;
                string attDType;
                fs >> attributeNum >> attDType;
                order->AddAttributeNum(i,attributeNum);
                if(attDType == "Int"){
                    order->AddAttributeType(i,Int);
                }else if (attDType == "Double"){
                    order->AddAttributeType(i,Double);
                }else if (attDType == "String"){
                    order->AddAttributeType(i,String);
                }else{
                    fs.close();
                    cout<<"Invalid Meta-Data Type"<<endl;
                    return 0;
                }
            }
            curDBObject = new SortedDBFile(order,runlen);
        }else{
            cout<<"Invalid File type";
            return 0;
        }
    }else{
        fs.close();
        return 0;
    }
    curDBObject->Open(f_path);
    fs.close();
    return 1;
}

//  Resets the pointer to first record and copies the pointed record to temporary Page
void DBFile::MoveFirst () {
    curDBObject->MoveFirst();
}

// Closes the already opened binary file
int DBFile::Close () {
    return curDBObject->Close();
}

// Adds record to the current page if current page has enough space 
// otherwise, it flushes current page to the binary file, frees the 
// current page, and appends the record to it.
void DBFile::Add (Record &rec) {
    curDBObject->Add(rec);
}

// It returns the next record from Page if it exists.
int DBFile::GetNext (Record &fetchme) {
    return curDBObject->GetNext(fetchme);
}

// It returns the next record from Page which satisfies the given parameter CNF if it exists.
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return curDBObject->GetNext(fetchme,cnf,literal);
}

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

// Heap File Constructor
HeapDBFile :: HeapDBFile(){
    curFile = new File();
    tempPage = new Page();
    isFileCreated = false;
    isFileOpened = false;
}

// Heap File Destructor
HeapDBFile :: ~HeapDBFile(){
    delete curFile;
    delete tempPage;
}

// This function creates the heap DB file with specified path
int HeapDBFile :: Create(const char *f_path){
    // cout<<"HeapDBFile:: Create : Start"<<endl;
    tempPage->EmptyItOut();
    curFile->Open(0,(char* )f_path); 
    isFileCreated = true;
    pIndex = 0;
    // cout<<"HeapDBFile:: Create : End"<<endl;
    return 1;
}

// This function loads the heap DB file with specified schema and load path
void HeapDBFile :: Load (Schema &f_schema, const char *loadpath) {
    FILE *loadFile = fopen(loadpath,"r");

    if (loadFile == NULL){
        cout<<loadpath<<" File doesn't exist"<<endl;
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

// This function opens the heap DB file with specified file path
int HeapDBFile::Open (const char *f_path) {
    pIndex = 0;
    curFile->Open(1,(char* )f_path);
    tempPage -> EmptyItOut();
    isFileOpened = true;
    return 1;
}

// This function moves the pointer to first in the heap DB file
void HeapDBFile::MoveFirst () {
    // cout<<"Start of MoveFirst Function"<<endl;
    pIndex = 0;
    tempPage -> EmptyItOut();
    curFile -> GetPage(tempPage,pIndex);
    // cout<<"End of MoveFirst Function"<<endl;
}

// This function closes the heap DB file
int HeapDBFile::Close () {
    // cout<<"Start of Close Function"<<endl;
    if( tempPage -> GetRecordsCnt() > 0){
        isFileOpened = false;
        curFile -> AddPage(tempPage, pIndex);
        tempPage -> EmptyItOut();
    }
    curFile->Close();
    // cout<<"End of Close Function"<<endl;
    return 1;
}

int heapcnt = 0;
// This function adds a record to the heap DB file
void HeapDBFile::Add (Record &rec) {
    // cout<<"HeapDBFile::Add : Start"<<endl;
    if(!tempPage -> Append(&rec)){
        curFile->AddPage(tempPage,pIndex++);
        tempPage ->EmptyItOut();
        tempPage ->Append(&rec);
        // cout<<"HeapDBFile::Add : IF"<<endl;
    }
    heapcnt++;
    // cout<<"HeapDBFile::Add : End count "<<heapcnt<<endl;
}

// This function gets the next record(points to) of the heap DB file
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

// This function gets the next record(points to) of the heap DB file with specified CNF
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



// Sorted DB File Constructor
SortedDBFile :: SortedDBFile(OrderMaker *o,int runlen){
    this->query = NULL;
    this->bq = NULL;
    this->curFile = new File();
    this->tempPage = new Page();
    this->o = o;
    this->bufSize = 1000;
    this->runlen = runlen;
}

// Sorted DB File Destructor
SortedDBFile :: ~SortedDBFile(){
    delete query;
    delete curFile;
    delete tempPage;
}

// This function creates the sorted DB file with specified path
int SortedDBFile :: Create(const char *f_path){
    writeMode = false;
    pIndex = 0;
    this->fpath = (char *)f_path;
    tempPage->EmptyItOut();
    curFile->Open(0,this->fpath);
    // cout<<"in SortedDBFile this path "<<this->fpath<<endl;
    return 1;
}

// This function loads the sorted DB file with specified schema and load path
void SortedDBFile :: Load (Schema &f_schema, const char *loadpath) {
    FILE *tFile = fopen(loadpath,"r");
    Record tempRecord;
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

// This function opens the sorted DB file with specified file path
int SortedDBFile::Open (const char *f_path) {
    writeMode = false;
    pIndex = 0;
    this->fpath = (char *)f_path;
    tempPage->EmptyItOut();
    // cout<<"SORTED::Open : Before Open func"<<endl;
    curFile->Open(1,this->fpath);
    // cout<<"SORTED::Open : After Open func"<<endl;
    if(curFile->GetLength()>0){
        curFile->GetPage(tempPage,pIndex);
    }
    return 1;
}

// This function moves the current pointer to first in the sorted DB file
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

// This function closes the sorted DB file
int SortedDBFile::Close () {
    if(writeMode){
        // cout<<"SORTED:: Close : before Merge"<<endl;
        MergeBigQToFile();
        // cout<<"SORTED:: Close : After Merge"<<endl;
    }
    // cout<<"SORTED:: Close : before file close"<<endl;
    curFile->Close();
    // cout<<"SORTED:: Close : After file close"<<endl;
    return 1;
}

// This function adds a record to the sorted DB file
void SortedDBFile::Add (Record &rec) {
    // cout<<"SORTED:: ADD : Start"<<endl;
    if(!writeMode){
        // cout<<"SORTED:: ADD : InitalizeBigQ"<<endl;
        InitializeBigQ ();
    }
    inpipe->Insert(&rec);
    // cout<<"SORTED:: ADD : End "<<endl;
}

// This function gets the next record(points to) the sorted DB file
int SortedDBFile::GetNext (Record &fetchRec) {
    // cout<<"SORTED::GetNext : start"<<endl;
    if(writeMode){
        MergeBigQToFile();
    }
    if(tempPage->GetFirst(&fetchRec)){
        // cout<<"SORTED::GetNext : return 1"<<endl;
        return 1;
    }else{
        pIndex++;
        if(pIndex<curFile->GetLength()-1){
            curFile->GetPage(tempPage,pIndex);
            tempPage->GetFirst(&fetchRec);
            // cout<<"SORTED::GetNext : return 1"<<endl;
            return 1;
        }else{
            // cout<<"SORTED::GetNext : return 0"<<endl;
            return 0;
        }
    }
    return 0;

}

// This function gets the next record(points to) the sorted DB file with specified CNF
int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if(writeMode){
        MergeBigQToFile();
    }
    ComparisonEngine compEng;
    // cout<<"SORTED:: GetNext : start"<<endl;
    if(!query){
        query = new OrderMaker;
        if (QueryOrderGenerator (*query, *o, cnf) > 0) {
			query->Print ();
			if (BinarySearch (fetchme, cnf, literal)) {
				return 1;
				
			} else {
				return 0;
				
			}
		} else {
			return GetNextSequential (fetchme, cnf, literal);
		}
    }else{
		if (query->numAtts == 0) {
			return GetNextSequential (fetchme, cnf, literal);
			
		} else {
			return GetNextWithQuery (fetchme, cnf, literal);
		}
    }
    return 0;
}

// This function merges the BigQ to the file
void SortedDBFile :: MergeBigQToFile(){
    inpipe->ShutDown();
    writeMode = false;
    if(curFile->GetLength()>0){
        MoveFirst();
    }
    // cout<<"SORTED:: MergeBigQToFile : after MoveFirst : curFile Len "<<curFile->GetLength()<<endl;
    Record *pipeRecord = new Record;
    Record *fileRecord = new Record;
    HeapDBFile *newHeap = new HeapDBFile;
    
    ComparisonEngine compEng;
    newHeap->Create("temp.bin");
    // cout<<"SORTED:: MergeBigQToFile : before GetNext"<<endl;
    int flagFile = GetNext(*fileRecord);
    // cout<<"SORTED:: MergeBigQToFile : after GetNext"<<endl;
    int flagPipe = outpipe->Remove(pipeRecord);
    // cout<<"SORTED:: MergeBigQToFile : before while"<<endl;
    while(flagFile&&flagPipe){
        if(compEng.Compare(pipeRecord,fileRecord,o)> 0){
            newHeap->Add(*fileRecord);
            flagFile = GetNext(*fileRecord);
        }else{
            newHeap->Add(*pipeRecord);
            flagPipe = outpipe->Remove(pipeRecord);
        }
    }
    while(flagFile){
        newHeap->Add(*fileRecord);
        flagFile = GetNext(*fileRecord);
        // cout<<"FROM FILE: "<<cnt<<endl;
    }
    while(flagPipe){
        newHeap->Add(*pipeRecord);
        // cout<<"BEFORE PIPE: "<<cnt<<endl;
        flagPipe = outpipe->Remove(pipeRecord);
        // cout<<"Flag Pipe "<<flagPipe<<endl;
        // cout<<"FROM PIPE: "<<cnt<<endl;
    }
    // cout<<"SORTED:: MergeBigQToFile : after while and records added "<<cnt<<endl;
    newHeap->Close();
    delete newHeap;
    curFile->Close();
    remove(fpath);
    rename("temp.bin",fpath);
    curFile->Open(1,fpath);
    MoveFirst();
}

// This function generates the query order based on CNF provided
int SortedDBFile :: QueryOrderGenerator (OrderMaker &query, OrderMaker &order, CNF &cnf){
    // cout<<"SORTED:: QueryOrderGenerator : start query "<<&query<<endl;
    query.numAtts = 0;
	bool gotIt = false;
	
	for (int i = 0; i < order.numAtts; ++i) {
		for (int j = 0; j < cnf.numAnds; ++j) {
			if (cnf.orLens[j] != 1 || cnf.orList[j][0].op != Equals ||
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
				query.numAtts++;	
				gotIt = true;
				break;
			}
        }
		if (!gotIt) {
			break;
		}
	}
    // cout<<"SORTED:: QueryOrderGenerator : End NumAtts "<<query.numAtts<<endl;
	return query.numAtts;
}

// This function searches/scans the record using CNF (Binary Search Algorithm)
int SortedDBFile :: BinarySearch(Record &fetchme, CNF &cnf, Record &literal){
    // cout<<"SORTED:: BinarySearch : start"<<endl;
    off_t start = pIndex;
	off_t end = curFile->GetLength () - 1;
	off_t middle = pIndex;
	
	Page *page = new Page;
	ComparisonEngine comp;

	while (true) {
		middle = (start + end) / 2;
		curFile->GetPage (page, middle);
		if (page->GetFirst (&fetchme)) {
			if (comp.Compare (&literal, query, &fetchme, o) <= 0) {
				end = middle - 1;
				// if (end <= start) break;
			} else {
				start = middle + 1;
				// if (end <= start) break;
			}
            if (end <= start) break;
		} 
        else break;
	}

	if (comp.Compare (&fetchme, &literal, &cnf)) {
		delete tempPage;
		pIndex = middle;
		tempPage = page;
		return 1;
	} else {
		delete page;
		return 0;
	}
}

// This function gets the next record sequentially based on the parameters
int SortedDBFile :: GetNextSequential (Record &fetchme, CNF &cnf, Record &literal){
    // cout<<"SORTED:: GetNextSequential : start"<<endl;
    ComparisonEngine compEng;
	while (GetNext (fetchme)) {
		if (compEng.Compare (&fetchme, &literal, &cnf)){
			return 1;
		}
	}
	return 0;
}

// This function gets the next record sequentially based on the query
int SortedDBFile :: GetNextWithQuery (Record &fetchme, CNF &cnf, Record &literal){
    // cout<<"SORTED:: GetNextWithQuery : start"<<endl;
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

// This function initializes BigQ
void SortedDBFile :: InitializeBigQ (){
    writeMode = true;
    inpipe = new Pipe(bufSize);
    outpipe = new Pipe(bufSize);
    // cout<<"SORTED:: InitializeBigQ : Start"<<endl;
    bq = new BigQ(*inpipe,*outpipe,*o,runlen);
    // pthread_create(&workerThread, NULL, StartMainThread, (void *)this);
	// pthread_join(workerThread,NULL);
    // cout<<"SORTED:: InitializeBigQ : End"<<endl;
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
    if(fname.size()==0){
        return -1;
    }
    fs.open(fname+".tmp");

    if (f_type == heap){
        fs << "heap" << endl;
        curDBObject = new HeapDBFile;

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
    // cout<<"In DBFILE OPEN START fname : "<<fname<<endl;
    fs.open(fname+".tmp");
    if(fs.is_open()){
        string ftype;
        fs >> ftype;
        if(ftype == "heap"){
            cout<<"Heap"<<endl;
            curDBObject = new HeapDBFile;
            curDBObject->Open(f_path);
        }else if (ftype == "sorted"){
            // cout<<"In DBFILE OPEN START : SORTED"<<endl;
            int runlen,numAtts;
            OrderMaker *order = new OrderMaker;
            fs >> runlen;
            fs >> numAtts;
            // cout<<"DBFILE::OPEN : runlen "<<runlen<<" numAtts "<<numAtts<<endl;
            order->numAtts = numAtts;
            // cout<<"DBFILE::OPEN : runlen "<<runlen<<" numAtts "<<numAtts<<endl;
            for( int i=0 ;i < numAtts; i++){
                int attributeNum;
                string attDType;
                fs >> attributeNum >> attDType;
                // cout<<"DBFILE::OPEN Attributes "<<attributeNum<<" "<<attDType<<endl;
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
            curDBObject->Open(f_path);
        }else{
            cout<<"Invalid File type";
            return 0;
        }
    }else{
        curDBObject = new HeapDBFile;
        curDBObject->Open(f_path);
        fs.close();
        return 0;
    }
    // curDBObject->Open(f_path);
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

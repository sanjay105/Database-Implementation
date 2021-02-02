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

// Constructor: initializes File and Page objects
DBFile::DBFile () {
    // cout<<"Start of DBFile constructor"<<endl;
    curFile = new File();
    tempPage = new Page();
    isFileCreated = false;
    isFileOpened = false;
    // cout<<"End of DBFile constructor"<<endl;
}

// Destructor: deletes File and Page objects
DBFile::~DBFile(){
    // cout<<"Start of DBFile destructor"<<endl;
    delete curFile;
    delete tempPage;
    // cout<<"End of DBFile destructor"<<endl;
}

// Creates a binary file at f_path of necessary file type 
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    // cout<<"Start of Create Function"<<endl;
    if (f_type == heap){
        tempPage->EmptyItOut();
        curFile->Open(0,(char* )f_path); //ytd
        isFileCreated = true;
        fileType = f_type;
        pIndex = 0;
    }else{
        // We can implement sorted and tree file type here
        cerr << "BAD. Invalid File type"<<endl;
        exit(2);
    }
    
    // cout<<"End of Create Function"<<endl;
    return 1;
}


// Loads the specifed DB table with its respective schema into a binary file
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    // cout<<"Start of Load Function"<<endl;

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

    // cout<<"End of Load Function"<<endl;

}

// Opens the binary file with the specified path and sets the pointer to first record
int DBFile::Open (const char *f_path) {
    // cout<<"Start of Open Function"<<endl;

    pIndex = 0;
    curFile->Open(1,(char* )f_path);
    tempPage -> EmptyItOut();
    isFileOpened = true;
    
    // cout<<"End of Open Function"<<endl;
    return 1;
}

//  Resets the pointer to first record and copies the pointed record to temporary Page
void DBFile::MoveFirst () {
    // cout<<"Start of MoveFirst Function"<<endl;

    pIndex = 0;
    tempPage -> EmptyItOut();
    curFile -> GetPage(tempPage,pIndex);

    // cout<<"End of MoveFirst Function"<<endl;
}

// Closes the already opened binary file
int DBFile::Close () {
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

// Adds record to the current page if current page has enough space 
// otherwise, it flushes current page to the binary file, frees the 
// current page, and appends the record to it.
void DBFile::Add (Record &rec) {
    // cout<<"Start of Add Function"<<endl;

    if(!tempPage -> Append(&rec)){
        curFile->AddPage(tempPage,pIndex++);
        tempPage ->EmptyItOut();
        tempPage ->Append(&rec);
    }

    // cout<<"End of Add Function"<<endl;
}

// It returns the next record from Page if it exists.
int DBFile::GetNext (Record &fetchme) {
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

// It returns the next record from Page which satisfies the given parameter CNF if it exists.
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
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

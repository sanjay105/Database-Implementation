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
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
    // cout<<"Start of DBFile constructor"<<endl;
    curFile = new File();
    tempPage = new Page();
    // cout<<"End of DBFile constructor"<<endl;
}

DBFile::~DBFile(){
    // cout<<"Start of DBFile destructor"<<endl;
    delete curFile;
    delete tempPage;
    // cout<<"End of DBFile destructor"<<endl;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    // cout<<"Start of Create Function"<<endl;
    tempPage->EmptyItOut();
    curFile->Open(0,(char* )f_path);
    isFileCreated = true;
    fileType = f_type;
    pIndex = 0;
    // cout<<"End of Create Function"<<endl;
    return 1;
}

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
    
    while(temp.SuckNextRecord(&f_schema,loadFile)==1){
        Add(temp);
    }
    
    curFile -> AddPage(tempPage,pIndex++);
    tempPage -> EmptyItOut();
    fclose(loadFile);

    // cout<<"End of Load Function"<<endl;

}

int DBFile::Open (const char *f_path) {
    // cout<<"Start of Open Function"<<endl;

    pIndex = 0;
    curFile->Open(1,(char* )f_path);
    tempPage -> EmptyItOut();
    
    // cout<<"End of Open Function"<<endl;
    return 0;
}

void DBFile::MoveFirst () {
    // cout<<"Start of MoveFirst Function"<<endl;

    pIndex = 0;
    tempPage -> EmptyItOut();
    curFile -> GetPage(tempPage,pIndex);

    // cout<<"End of MoveFirst Function"<<endl;
}

int DBFile::Close () {
    // cout<<"Start of Close Function"<<endl;

    if(isFileCreated && tempPage -> GetRecordsCnt() > 0){
        isFileCreated = false;
        curFile -> AddPage(tempPage, pIndex);
        tempPage -> EmptyItOut();
    }

    curFile->Close();
    
    // cout<<"End of Close Function"<<endl;
    return 1;
}

void DBFile::Add (Record &rec) {
    // cout<<"Start of Add Function"<<endl;

    if(!tempPage -> Append(&rec)){
        curFile->AddPage(tempPage,pIndex++);
        tempPage ->EmptyItOut();
        tempPage ->Append(&rec);
    }

    // cout<<"End of Add Function"<<endl;
}

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

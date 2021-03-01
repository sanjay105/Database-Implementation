#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class Comparer {
	
private:
	
	OrderMaker *sortorder;
	
public:
	
	Comparer (OrderMaker *order);
	bool operator() (Record* left, Record* right);
	
};


class Run {
	
private:
	
	Page bufferPage;
	
public:
	
	Record* firstRecord;
    OrderMaker* sortorder;
    File *runsFile;
	
    int pageOffset;
    int runSize;
	
	Run (int runSize, int pageOffset, File *file, OrderMaker *order);
	Run (File *file, OrderMaker *order);
    ~Run();
    
    int GetFirstRecord();
	
};

class Compare {
	
private:
	
    OrderMaker *sortorder;
	
public:
	
    bool operator() (Run* left, Run* right);
	
}; 

// simple struct to pass variables from BigQ to its worker thread
typedef struct {
	
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
	
} BigQInfo;

class BigQ {

private:
	int totalPages;
    Pipe *inputPipe;
    Pipe *outputPipe;
    pthread_t workerThread;
    char *fileName;
    priority_queue<Run*, vector<Run*>, Compare> runQueue;
    OrderMaker *sortorder;
	
    bool WriteRunToFile ();
    void AddRunToQueue (int runSize, int pageOffset);
    friend bool comparer (Record* left, Record* right);

public:
	
	File runsFile;
    vector<Record*> recordList;
    int runlength;
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ () {};
	
	void SortsListOfRecord();
    void CombineRuns ();
	
    static void *StartMainThread (void *start) {
		
        BigQ *bigQ = (BigQ *)start;
        bigQ->SortsListOfRecord ();
        bigQ->CombineRuns ();
		return NULL;
    }
	
};

#endif
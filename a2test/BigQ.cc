#include "BigQ.h"
#include <bits/stdc++.h>

OrderedCompare :: OrderedCompare(OrderMaker &sortorder){
	order = sortorder;
}

bool OrderedCompare :: operator() (Record *a, Record *b){
	ComparisonEngine comp;
	return comp.Compare(a,b,&order)!=1;
}

OrderedCompare :: ~OrderedCompare(){

}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	pthread_t workerthread;
	struct TPPMSParams params = {in,out,sortorder,runlen};
	int tstatus = pthread_create(&workerthread,NULL,TPPMS,(void *)&params);
	if(tstatus != 0){
		// if pthread_create fails exits with return code why it failed
		exit(tstatus);
	}
	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

int WriteSortedBufferToFile(vector<Record *> &buffer,int pindex, File *file){
	Page page;
	page.EmptyItOut();
	for(auto itr = buffer.begin();itr!=buffer.end();itr++){
		if(page.Append(*itr)==0){
			file->AddPage(&page,pindex++);
			page.EmptyItOut();
			page.Append(*itr);
		}
	}
	if (page.GetRecordsCnt() > 0){
		file->AddPage(&page,pindex++);
		page.EmptyItOut();
	}
	return pindex;
}

void *TPPMS(void *args){
	struct TPPMSParams *p = (struct TPPMSParams *) args;
	OrderedCompare comp(p->sortorder);
	vector<Record *> buffer;
	vector<off_t> runindexes;
	off_t pindex = 0;
	File file;
	
	Record record;
	Record *nextrecord;
	int cursize = sizeof(int);
	int maxallowedsize = PAGE_SIZE * p->runlen;

	char *tempfname = "temp.bin";
	file.Open(0,tempfname);
	
	// Sort individual blocks
	while(p->in.Remove(&record) == 1){
		nextrecord = new Record;
		nextrecord->Consume(&record);
		cursize += nextrecord->Size();
		
		if (cursize > maxallowedsize){
			sort(buffer.begin(),buffer.end(),comp);
			pindex = WriteSortedBufferToFile(buffer,pindex,&file);
			runindexes.push_back(pindex);
			buffer.clear();
			cursize = sizeof(int) + nextrecord->Size();
		}
		buffer.push_back(nextrecord);
	}
	sort(buffer.begin(),buffer.end(),comp);
	pindex = WriteSortedBufferToFile(buffer,pindex,&file);
	runindexes.push_back(pindex);
	buffer.clear();

	// Merge all blocks using Priority Queue




}

BigQ::~BigQ () {
}

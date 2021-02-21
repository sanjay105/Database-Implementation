#include "BigQ.h"
#include "Comparison.h"
#include <bits/stdc++.h>


// Constructor for the RecordPQ class
// RecordPQ object is used as data object in Priority Queue
RecordPQ :: RecordPQ(off_t curoffset,off_t eoffset,File *fp){
	tempPage = new Page;
	record = new Record;

	offset = curoffset;
	endoffset = eoffset;
	file = fp;
	// cout<<"Here"<<endl;
	file->GetPage(tempPage,offset++);
	tempPage->GetFirst(record);
}

// Gets the next record in the run and returns true otherwise it returns false
bool RecordPQ :: GetNextRecord(Record *rec){
	// record->Print();
	rec->Consume(record);
	if ( tempPage->GetFirst(record) == 1){
		return true;
	}else{
		if(offset < endoffset){
			
			file->GetPage(tempPage,offset++);
			tempPage->GetFirst(record);
			return true;
		}
		return false;
	}
}

// RecordPQ destructor
RecordPQ :: ~RecordPQ(){

}

// OrderedCompare class constructor and initializes the Ordermaker
OrderedCompare :: OrderedCompare(OrderMaker *sortorder){
	order = sortorder;
}

// Overloading the compare operator to perform sort
bool OrderedCompare :: operator() (Record *a, Record *b){
	ComparisonEngine comp;
	return comp.Compare(a,b,order)<0;
}

// Overloading the compare operator to perform sort in Priority Queue
bool OrderedCompare :: operator()(RecordPQ *a,RecordPQ *b){
	ComparisonEngine comp;
	return comp.Compare(a->record,b->record,order)>0;
}

// OrderedCompare class destructor
OrderedCompare :: ~OrderedCompare(){

}

// Function to write Sorted run into temporary file
int WriteSortedBufferToFile(vector<Record *> &buffer,int pindex, File *file, Page *page){

	for(auto itr = buffer.begin();itr!=buffer.end();itr++){
		if(page->Append(*itr)==0){
			file->AddPage(page,pindex++);
			page->EmptyItOut();
			page->Append(*itr);
		}
	}
	if (page->GetRecordsCnt() > 0){
		file->AddPage(page,pindex++);
		page->EmptyItOut();
	}
	return pindex;
}

// Sort the records in the vector using OrderMaker
void SortRecords(vector<Record *> &records, OrderMaker *om){
	OrderedCompare comp(om);
	sort(records.begin(),records.end(),comp);
}

// Function to perform TPMMS algorithm
void *TPMMS(void *args){
	struct TPMMSParams *pim = (struct TPMMSParams *) args;
	struct TPMMSParams p={pim->in,pim->out,pim->sortorder,pim->runlen};
	OrderedCompare comp(&p.sortorder);
	vector<Record *> buffer;
	vector<off_t> runindexes;
	off_t pindex = 0;
	File file;
	Record record;
	Record *nextrecord;
	long long int cursize = sizeof(int);
	long long int maxallowedsize = PAGE_SIZE * p.runlen;

	char *tempfname = "temp.bin";
	file.Open(0,tempfname);
	int i=0;
	Page page;
	page.EmptyItOut();
	while(p.in.Remove(&record) == 1){
		
		i++;
		nextrecord = new Record;
		nextrecord->Consume(&record);
		cursize += nextrecord->Size();
		
		if (cursize > maxallowedsize){
			// if current run buffer is full
			// sort(buffer.begin(),buffer.end(),comp);
			SortRecords(buffer,&p.sortorder);
			pindex = WriteSortedBufferToFile(buffer,pindex,&file,&page);
			cursize = sizeof(int) + nextrecord->Size();
			buffer.clear();
			runindexes.push_back(pindex);
		}
		buffer.push_back(nextrecord);
	}
	
	// sort(buffer.begin(),buffer.end(),comp);
	SortRecords(buffer,&p.sortorder);
	pindex = WriteSortedBufferToFile(buffer,pindex,&file,&page);
	runindexes.push_back(pindex);
	buffer.clear();

	// Merge all blocks using Priority Queue

	priority_queue<RecordPQ *,vector<RecordPQ *>,OrderedCompare> pq(comp);
	off_t curoffset = 0;
	int cnt = 0;
	for(off_t i:runindexes){
		pq.push(new RecordPQ(curoffset,i,&file));
		cnt++;
		curoffset = i;
	}
	int j = 0;
	while(!pq.empty()){
		RecordPQ *cur = pq.top();
		pq.pop();
		if(cur->GetNextRecord(&record)){
			pq.push(cur);
		}
		p.out.Insert(&record);
		j++;
	}
	p.out.ShutDown();
	file.Close();
	return NULL;
}

// Constructor for BigQ class, creates a worker thread to run TPMMS algorithm
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	if(runlen < 1){
		cerr << "runlen Inavlid";
		exit(7);
		
	}
	if(in.GetDone()==1){
		cerr << "in pipe is closed";
		exit(7);
	}
	if(out.GetDone()==1){
		cerr << "out pipe is closed";
		exit(7);
	}
	pthread_t workerthread;
	 
	struct TPMMSParams params={in,out,sortorder,runlen};
	int tstatus = pthread_create(&workerthread,NULL,TPMMS,(void *)&params);
	if(tstatus != 0){
		// if pthread_create fails exits with return code why it failed
		exit(tstatus);
	}
	// Waits until worker thread joins back
	pthread_join(workerthread,NULL);
	exit(200);
}


// BigQ destructor
BigQ::~BigQ () {

}

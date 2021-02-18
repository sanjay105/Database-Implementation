#include "BigQ.h"
#include "Comparison.h"
#include <bits/stdc++.h>



RecordPQ :: RecordPQ(off_t curoffset,off_t endoffset,File *fp){
	tempPage = new Page;
	record = new Record;

	offset = curoffset;
	endoffset = endoffset;
	file = fp;
	// cout<<"Here"<<endl;
	file->GetPage(tempPage,offset++);
	tempPage->GetFirst(record);
}

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

RecordPQ :: ~RecordPQ(){

}

OrderedCompare :: OrderedCompare(OrderMaker *sortorder){
	order = sortorder;
}

bool OrderedCompare :: operator() (Record *a, Record *b){
	ComparisonEngine comp;
	// cout<<"Comparingg...."<<endl;
	// char *catalog_path = "catalog"; 
	// char *nation = "customer"; 
	cout<<a<<endl;
	cout<<b<<endl;
	// a->Print(new Schema (catalog_path, nation));
	// b->Print(new Schema (catalog_path, nation));
	// cout<<"Comapre "<<comp.Compare(a,b,order)<<endl;
	return comp.Compare(a,b,order)!=1;
}

bool OrderedCompare :: operator()(RecordPQ *a,RecordPQ *b){
	ComparisonEngine comp;
	return comp.Compare(a->record,b->record,order)!=1;
}

OrderedCompare :: ~OrderedCompare(){

}

void printBufferedRecords(vector<Record *> &buffer){
	cout<<"Printing Buffered Records"<<endl;
	char *catalog_path = "catalog"; 
	char *nation = "customer"; 
	for(auto itr:buffer){
		cout<<itr;
		itr->Print(new Schema (catalog_path, nation));
	}
	cout<<"Printing Done "<<endl;
}

void printNationRecord(Record *a){
	char *catalog_path = "catalog"; 
	char *nation = "nation"; 
	Schema *nationS = new Schema (catalog_path, nation);
	a->Print(nationS);
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
	struct TPPMSParams *pim = (struct TPPMSParams *) args;
	struct TPPMSParams p={pim->in,pim->out,pim->sortorder,pim->runlen};
	// struct TPPMSParams p = {pim->in,pim->out,pim->sortorder,pim->runlen};
	OrderedCompare comp(&p.sortorder);
	vector<Record *> buffer;
	vector<off_t> runindexes;
	off_t pindex = 0;
	File file;
	// cout<<" In Pipe Status "<<&p->in<<endl;
	// cout<<" Out Pipe Status "<<&p->out<<endl;
	// cout<<" Run Length "<<p->runlen<<endl;
	// cout<<" Page size "<<PAGE_SIZE<<endl;
	// vector<Pipe *> temp = *(p->pipes);
	// cout<<"here"<<endl;
	// (*(p->pipes))[0]->Remove
	// cout<<" Output Pipe "<<&outputpipe<<endl;
	Record record;
	Record *nextrecord;
	long long int cursize = sizeof(int);
	// cout<<"Page Size : "<<PAGE_SIZE<<endl;
	long long int maxallowedsize = PAGE_SIZE * p.runlen;

	char *tempfname = "temp.bin";
	file.Open(0,tempfname);
	// Sort individual blocks
	// p->pipes[0]->Remove
	int i=0;
	// cout<<"Here ";cout<<(*(p->pipes))[0]<<endl;
	// cout<<p->in.GetDone();cout<<"-"<<p->out.GetDone()<<endl;
	while(p.in.Remove(&record) == 1){
		cout<<"Loaded record from pipe " <<endl;
		nextrecord = new Record;
		nextrecord->Consume(&record);
		cursize += nextrecord->Size();
		cout<<"Current Size "<<cursize<<" and Max allowed size "<<maxallowedsize<<endl;
		if (cursize > maxallowedsize){
			cout<<"Writing Chunck "<<++i<<endl;
			// printBufferedRecords(buffer);
			sort(buffer.begin(),buffer.end(),comp);
			cout<<"Sort done"<<endl;
			pindex = WriteSortedBufferToFile(buffer,pindex,&file);
			cursize = sizeof(int) + nextrecord->Size();
			buffer.clear();
			runindexes.push_back(pindex);
		}
		buffer.push_back(nextrecord);
		// cout<<"Buffer Size : "<<buffer.size()<<endl;
	}
	// cout<<"Loading Records from PIPE Done"<<endl;
	
	sort(buffer.begin(),buffer.end(),comp);
	// cout<<"Buffer Sorted "<<endl;
	// printBufferedRecords(buffer);
	pindex = WriteSortedBufferToFile(buffer,pindex,&file);
	// cout<<"Sorted to File"<<endl;
	runindexes.push_back(pindex);
	buffer.clear();
	cout<<"********************** Part1 Done **********************"<<endl;
	// Merge all blocks using Priority Queue
	priority_queue<RecordPQ *,vector<RecordPQ *>,OrderedCompare> pq(comp);
	off_t curoffset = 0;
	int cnt = 0;
	for(off_t i:runindexes){
		pq.push(new RecordPQ(curoffset,i,&file));
		cnt++;
		// cout<<"Added to PQ : "<<cnt<<" records from "<<curoffset<<" to "<<i<<endl;
		curoffset = i;
	}
	// cout<<"Here"<<endl;
	while(!pq.empty()){
		RecordPQ *cur = pq.top();
		
		pq.pop();

		if(cur->GetNextRecord(&record)){
			pq.push(cur);
		}
		// cout<<"After GetNextRecord"<<endl;
		// printNationRecord(&record);
		// cout<<"Priority Queue Size : "<<pq.size()<<endl;
		// cout<<"Out Pipe Status"<<temp[1]->GetDone()<<endl;
		p.out.Insert(&record);
		// cout<<"After Inserting into pipe"<<endl;
	}
	p.out.ShutDown();
	file.Close();
	return NULL;
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	pthread_t workerthread;
	 
	struct TPPMSParams params={in,out,sortorder,runlen};
	// params.in = in;
	// params.out = out;
	// params.sortorder = sortorder;
	// params.runlen = runlen;
	// cout<<"Runlen: "<<params.runlen<<endl;
	// cout<<" In Pipe Status "<<&params.in<<endl<<" Out Pipe Status "<<&params.out<<endl;
	// cout<<in.GetDone();cout<<"-"<<out.GetDone()<<endl;
	// cout<<"Run Length : "<<runlen<<endl;
	int tstatus = pthread_create(&workerthread,NULL,TPPMS,(void *)&params);
	if(tstatus != 0){
		// if pthread_create fails exits with return code why it failed
		exit(tstatus);
	}
	pthread_join(workerthread,NULL);
	// this_thread::sleep_for(2000ms);
	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	
}



BigQ::~BigQ () {
}

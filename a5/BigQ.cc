#include "BigQ.h"
#include <unistd.h>

// sortorder initialization
Comparer :: Comparer (OrderMaker *order) {
    sortorder = order;
}

// comparator function
bool Comparer::operator() (Record* left, Record* right) {
	ComparisonEngine compEng;
    if (compEng.Compare (left, right, sortorder) < 0) 
        return true;
    return false;
}

// This function implements the compare
bool Compare :: operator() (Run* left, Run* right) {
    ComparisonEngine compEng;
    if (compEng.Compare (left->firstRecord, right->firstRecord, left->sortorder) < 0)
        return false;
    return true;
	
}

// This constructor initializes the fields inside the Run class with specified run length
Run :: Run (int run_length, int page_offset, File *file, OrderMaker* order) {
    runSize = run_length;
    pageOffset = page_offset;
    firstRecord = new Record ();
    runsFile = file;
    sortorder = order;
    runsFile->GetPage (&bufferPage, pageOffset);
    GetFirstRecord ();
}

// This constructor initializes the fields inside the Run class
Run :: Run (File *file, OrderMaker *order) {
    firstRecord = NULL;
    runsFile = file;
    sortorder = order;
}

// Destructs the Run class (deletes the first record)
Run :: ~Run () {
    delete firstRecord;
}

// This function gets the first record
int Run :: GetFirstRecord () {
    if(runSize <= 0) {
        return 0;
    }
    Record* record = new Record();
    if (bufferPage.GetFirst(record) == 0) {
        pageOffset++;
        runsFile->GetPage(&bufferPage, pageOffset);
        bufferPage.GetFirst(record);
    }
    runSize--;
    firstRecord->Consume(record);
    return 1;
}

// This function writes the run to the file
bool BigQ :: WriteRunToFile () {
    Page* page = new Page();
    int recLSize = recordList.size();
    int firstPageOffset = totalPages;
    int pageCnt = 1;
    
    for (int i = 0; i < recLSize; i++) {
        Record* record = recordList[i];
        if ((page->Append (record)) == 0) {
            pageCnt++;
            runsFile.AddPage (page, totalPages);
            totalPages++;
            page->EmptyItOut ();
            page->Append (record);
        }
        delete record;
    }
    runsFile.AddPage(page, totalPages);
    totalPages++;
    page->EmptyItOut();
    recordList.clear();
    delete page;
    AddRunToQueue (recLSize, firstPageOffset);
    return true;
}

// Adds the Run to the Priority Queue
void BigQ :: AddRunToQueue (int runLength, int pageOffset) {
    Run* run = new Run(runLength, pageOffset, &runsFile, sortorder);
    runQueue.push(run);
}

int inputCount = 0;
// This function sorts the record list
void BigQ :: SortsListOfRecord() {
    Page* page = new Page();
    int pageCnt = 0, recCnt = 0; 
    Record* record = new Record();
    srand (time(NULL));
    fileName = new char[100];
    sprintf (fileName, "%d.txt", (int)workerThread);
    runsFile.Open (0, fileName);
    
    while (inputPipe->Remove(record)) {
		inputCount++ ;
        Record* copyOfRecord = new Record ();
		copyOfRecord->Copy (record);
		recCnt++;
        if (page->Append (record) == 0) {
            pageCnt++;
            if (pageCnt == runlength) {
                sort (recordList.begin (), recordList.end (), Comparer (sortorder));
                int recordListSize = recordList.size ();
                WriteRunToFile (); 
                pageCnt = 0;
            }
            page->EmptyItOut ();
            page->Append (record);	
        }
        recordList.push_back (copyOfRecord);
    }
    // cout<<"BigQ :: SortRecordList : incount "<<incnt<<endl;
    if(recordList.size () > 0) {
        sort (recordList.begin (), recordList.end (), Comparer (sortorder));
        int recordListSize = recordList.size ();
        WriteRunToFile ();
        page->EmptyItOut ();
    }
    delete record;
    delete page;
}

int outputCount = 0;
// This function combines all the runs
void BigQ :: CombineRuns () {    
    Run* run = new Run (&runsFile, sortorder);
    Page page;

    int i = 0;
    while (!runQueue.empty ()) {
        Record* record = new Record ();
        run = runQueue.top ();
        runQueue.pop ();
        record->Copy (run->firstRecord);
        outputPipe->Insert (record);
		outputCount++;
        if (run->GetFirstRecord () > 0) {
            runQueue.push(run);
		}
        delete record;
    }
    runsFile.Close();
    remove(fileName);
    // cout<<"BigQ :: MergeRuns : outcount "<<outcnt<<endl;
    // usleep(10000000);
    outputPipe->ShutDown();
    // delete run;
}

// Constructor initializes the fields
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    this->sortorder = &sortorder;
    inputPipe = &in;
    outputPipe = &out;
    runlength = runlen;
    totalPages = 1;
    pthread_create(&workerThread, NULL, StartMainThread, (void *)this);
	// pthread_join(workerThread,NULL);
}
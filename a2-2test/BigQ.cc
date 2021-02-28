#include "BigQ.h"
#include <unistd.h>
/* Comparer Implementation */
Comparer :: Comparer (OrderMaker *order) {
	
    sortorder = order;
	
}

bool Comparer::operator() (Record* left, Record* right) {
    
	ComparisonEngine comparisonEngine;
    
    if (comparisonEngine.Compare (left, right, sortorder) < 0) {
		
        return true;
		
	} else {
		
		return false;
		
	}
	
}

/* Compare Implementation */
bool Compare :: operator() (Run* left, Run* right) {
	
    ComparisonEngine comparisonEngine;
    
    if (comparisonEngine.Compare (left->firstRecord, right->firstRecord, left->sortorder) < 0) {
		
        return false;
		
    } else {
		
        return true;
		
	}
	
}

/* Run Implementation */
Run :: Run (int run_length, int page_offset, File *file, OrderMaker* order) {
    
    runSize = run_length;
    pageOffset = page_offset;
    firstRecord = new Record ();
    runsFile = file;
    sortorder = order;
    runsFile->GetPage (&bufferPage, pageOffset);
    GetFirstRecord ();
	
}

Run :: Run (File *file, OrderMaker *order) {
	
    firstRecord = NULL;
    runsFile = file;
    sortorder = order;
	
}

Run :: ~Run () {
	
    delete firstRecord;
	
}

int Run :: GetFirstRecord () {
    
    if(runSize <= 0) {
        return 0;
    }
    
    Record* record = new Record();
    
    // try to get the Record, get next page if necessary
    if (bufferPage.GetFirst(record) == 0) {
        pageOffset++;
        runsFile->GetPage(&bufferPage, pageOffset);
        bufferPage.GetFirst(record);
    }
    
    runSize--;
    
    firstRecord->Consume(record);
    
    return 1;
}

/* BigQ Implementation */
bool BigQ :: WriteRunToFile (int runLocation) {
    
    Page* page = new Page();
    int recordListSize = recordList.size();
    
    int firstPageOffset = totalPages;
    int pageCounter = 1;
    
    for (int i = 0; i < recordListSize; i++) {
		
        Record* record = recordList[i];
		
        if ((page->Append (record)) == 0) {
            
            pageCounter++;
            
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
	
    AddRunToQueue (recordListSize, firstPageOffset);
    return true;
	
}

void BigQ :: AddRunToQueue (int runSize, int pageOffset) {
	
    Run* run = new Run(runSize, pageOffset, &runsFile, sortorder);
    runQueue.push(run);
	
}
int incnt = 0;
void BigQ :: SortRecordList() {
	
    Page* page = new Page();
    int pageCounter = 0, recordCounter = 0, currentRunLocation = 0;
	
    Record* record = new Record();
	
    srand (time(NULL));
    fileName = new char[100];
    sprintf (fileName, "%d.txt", (int)workerThread);
    
    runsFile.Open (0, fileName);
    
    while (inputPipe->Remove(record)) {
		incnt++ ;
        Record* copyOfRecord = new Record ();
		copyOfRecord->Copy (record);
        
		recordCounter++;
        
        if (page->Append (record) == 0) {
            
            pageCounter++;
			
            if (pageCounter == runlength) {
                
                sort (recordList.begin (), recordList.end (), Comparer (sortorder));
                currentRunLocation = (runsFile.GetLength () == 0) ? 0 : (runsFile.GetLength () - 1);
                
                int recordListSize = recordList.size ();
                
                WriteRunToFile (currentRunLocation); 
                
                pageCounter = 0;
				
            }
			
            page->EmptyItOut ();
            page->Append (record);
			
        }
        
        recordList.push_back (copyOfRecord);
        
    }
    // cout<<"BigQ :: SortRecordList : incount "<<incnt<<endl;
    
    
    // Last Run
    if(recordList.size () > 0) {
		
        sort (recordList.begin (), recordList.end (), Comparer (sortorder));
        currentRunLocation = (runsFile.GetLength () == 0) ? 0 : (runsFile.GetLength () - 1);
        
        int recordListSize = recordList.size ();
		
        WriteRunToFile (currentRunLocation);
        
        page->EmptyItOut ();
		
    }
	
    delete record;
    delete page;
	
}
int outcnt = 0;
void BigQ :: MergeRuns () {
    
    Run* run = new Run (&runsFile, sortorder);
    Page page;
    
    int i = 0;
	
    while (!runQueue.empty ()) {
		
        Record* record = new Record ();
        run = runQueue.top ();
        runQueue.pop ();
            
        record->Copy (run->firstRecord);
        outputPipe->Insert (record);
		outcnt++;
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

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
    this->sortorder = &sortorder;
    inputPipe = &in;
    outputPipe = &out;
    runlength = runlen;
    totalPages = 1;
    
    pthread_create(&workerThread, NULL, StartMainThread, (void *)this);
	// pthread_join(workerThread,NULL);
    
}
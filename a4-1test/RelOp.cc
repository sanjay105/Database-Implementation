#include "RelOp.h"

int bSize = 100;

/*
This function keeps the current thread waiting until the specified relational operation thread is finished its execution.
*/
void RelationalOp::WaitUntilDone (){
	// cout<<"RelationalOp:WaitUntilDone : START"<<endl;
	pthread_join(th,NULL);
	// cout<<"RelationalOp:WaitUntilDone : END"<<endl;
}

void RelationalOp::Use_n_Pages(int runlen){
	runLen = runlen;
}

/*
It starts the specific relational operator function where relational operation is chosen by the type of ‘obj’ during runtime.
*/
void *performOperation(void *obj){
	((RelationalOp *)obj)->Start();
	return NULL;
}

/*
This function initializes all the variables needed to perform select file operation and spawns the thread which performs select file operation.
*/
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	// cout<<"SelectFile:Run : START"<<endl;
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	pthread_create(&th,NULL,performOperation, (void *)this);
	// cout<<"SelectFile:Run : END"<<endl;
}

/*
This function gets all the records from the specified DB file and inserts the records into an out pipe which satisfies the given CNF.
*/
void SelectFile::Start(){
	// cout<<"SelectFile:Start : START"<<endl;
	ComparisonEngine comp;
	Record *rec = new Record();
	this->inFile->MoveFirst();
	// cout<<"SelectFile:Start : inFile : "<<this->inFile<<endl;
	int cnt = 0;
	// while(this->inFile->GetNext(*rec)){
	// 	// cout<<"SelectFile:Start : Record : "<<rec->Size()<<endl;
	// 	if(comp.Compare(rec,this->literal,this->selOp)){
	// 		this->outPipe->Insert(rec);
	// 		cnt++;
	// 	}
	// }
	
	while(this->inFile->GetNext(*rec,*this->selOp,*this->literal)){
		this->outPipe->Insert(rec);
		cnt++;
	}
	// cout<<cnt<<" Records Selected"<<endl;
	this->outPipe->ShutDown();
	// cout<<"SelectFile:Start : END"<<endl;

}

/*
This function initializes all the variables needed to perform select pipe operation and spawns the thread which performs select pipe operation.
*/
void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	pthread_create(&th,NULL,performOperation, (void *) this);
}


/*
This function gets all the records from the inpipe and inserts the records into an out pipe which satisfies the given CNF.
*/
void SelectPipe::Start(){
	ComparisonEngine comp;
	Record *rec = new Record();

	while(this->inPipe->Remove(rec)){
		if(comp.Compare(rec,this->literal,this->selOp)){
			this->outPipe->Insert(rec);
		}
	}
	this->outPipe->ShutDown();

}

/*
This function initializes all the variables needed to perform project relational operation and spawns the thread which performs project relational operation.
*/
void Project :: Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;

	pthread_create(&th,NULL,performOperation, (void *) this);
}


/*
This function gets all the records from the inpipe and inserts the records into an out pipe with specified attributes (subset of actual attributes of a record).
*/
void Project :: Start(){
	Record *rec = new Record();
	int cnt = 0;
	while(this->inPipe->Remove(rec)){
		rec->Project(this->keepMe,this->numAttsOutput,this->numAttsInput);
		this->outPipe->Insert(rec);
		cnt++;
	}
	// cout<<"Projected "<<cnt<<" Records"<<endl;
	this->outPipe->ShutDown();
}


/*
This function initializes all the variables needed to perform join relational operation and spawns the thread which performs join relational operation.
*/
void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

/*
This function joins all the records from the inpipe left and inpipe right which satisfies the given CNF and inserts the resultant records into the out pipe.
*/
void Join :: Start(){
	int count = 0;
	
	int leftNumAtts, rightNumAtts;
	int totalNumAtts;
	int *attsToKeep;
	
	ComparisonEngine comp;
	
	Record *fromLeft = new Record ();
	Record *fromRight = new Record ();
	
	OrderMaker leftOrder, rightOrder;
	
	this->selOp->GetSortOrders(leftOrder, rightOrder);
	
	if (leftOrder.numAtts > 0 && rightOrder.numAtts > 0) {
		Pipe left (bSize);
		Pipe right (bSize);
		BigQ bigqLeft (*this->inPipeL, left, leftOrder, runLen);
		BigQ bigqRight (*this->inPipeR, right, rightOrder, runLen);
		bool isDone = false;
		
		if (!left.Remove (fromLeft)) {
			isDone = true;
		} else {
			leftNumAtts = fromLeft->GetLength ();
		}
		if (!isDone && !right.Remove (fromRight)) {
			isDone = true;
		} else {
			rightNumAtts = fromRight->GetLength ();
			totalNumAtts = leftNumAtts + rightNumAtts;
			attsToKeep = new int[totalNumAtts];
			
			for (int i = 0; i < leftNumAtts; i++) {
				attsToKeep[i] = i;
			}
			for (int i = 0; i < rightNumAtts; i++) {
				attsToKeep[leftNumAtts + i] = i;
			}
		}
		while (!isDone) {
			while (comp.Compare (fromLeft, &leftOrder, fromRight, &rightOrder) > 0) {
				if (!right.Remove (fromRight)) {
					isDone = true;
					break;
				}
			}
			while (!isDone && comp.Compare (fromLeft, &leftOrder, fromRight, &rightOrder) < 0) {
				if (!left.Remove (fromLeft)) {
					isDone = true;
					break;
				}
			}
			while (!isDone && comp.Compare (fromLeft, &leftOrder, fromRight, &rightOrder) == 0) {
				Record *tmp = new Record ();
				tmp->MergeRecords (fromLeft,fromRight,leftNumAtts,rightNumAtts,attsToKeep,totalNumAtts,leftNumAtts);
				this->outPipe->Insert (tmp);
				if (!right.Remove (fromRight)) {
					isDone = true;
					break;
				}
			}
		}
		while (right.Remove (fromLeft));
		while (left.Remove (fromLeft));
		
	} else {
		char fileName[100];
		sprintf (fileName, "temp.tmp");
		HeapDBFile dbFile;
		dbFile.Create (fileName);
		bool isDone = false;
		if (!this->inPipeL->Remove (fromLeft)) {
			isDone = true;
		} else {
			leftNumAtts = fromLeft->GetLength ();
		}
		if (!this->inPipeR->Remove (fromRight)) {
			isDone = true;
		} else {
			rightNumAtts = fromRight->GetLength ();
			totalNumAtts = leftNumAtts + rightNumAtts;
			attsToKeep = new int[totalNumAtts];
			for (int i = 0; i < leftNumAtts; i++) {
				attsToKeep[i] = i;
			}
			for (int i = 0; i < rightNumAtts; i++) {
				attsToKeep[leftNumAtts + i] = i;
			}
		}
		if (!isDone) {
			do{
				dbFile.Add (*fromLeft);
			} while (this->inPipeL->Remove (fromLeft));
			do{
				dbFile.MoveFirst ();
				Record *newRec = new Record ();
				while (dbFile.GetNext (*fromLeft)) {
					if (comp.Compare (fromLeft, fromRight, this->literal,this->selOp)) {
						newRec->MergeRecords (fromLeft,fromRight,leftNumAtts,rightNumAtts,attsToKeep,totalNumAtts,leftNumAtts);
						this->outPipe->Insert (newRec);
					}
				}
				delete newRec;
			} while (this->inPipeR->Remove (fromRight));
		}
		dbFile.Close ();
		remove ("temp.tmp");
	}
	
	this->outPipe->ShutDown ();
	
}

/*
This function initializes all the variables needed to perform distinct relational operation and spawns the thread which performs distinct relational operation.
*/
void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

/*
This function gets all the records from the inpipe and returns the distinct records into an out pipe.
*/
void DuplicateRemoval :: Start(){
	OrderMaker om(this->mySchema);
	ComparisonEngine comp;
	Record *prev = new Record();
	Record *next = new Record();
	Pipe tp(bSize);
	BigQ bq(*this->inPipe,tp,om,runLen);
	tp.Remove(prev);
	while(tp.Remove(next)){
		if(comp.Compare(prev,next,&om)){
			this->outPipe->Insert(prev);
			prev->Copy(next);
		}
	}
	if (next->bits != NULL && !comp.Compare (next, prev, &om)) {
		
		this->outPipe->Insert (prev);
		prev->Copy (next);
		
	}
	
	this->outPipe->ShutDown ();
}

/*
This function initializes all the variables needed to perform sum relational operation and spawns the thread which performs sum relational operation.
*/
void Sum :: Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

/*
This function gets all the records from the inpipe and returns the sum of the given attributes as a single record into an out pipe.
*/
void Sum :: Start(){
	Record *rec = new Record();
	Attribute curAtt;
	curAtt.name = "SUM";
	int runIntSum = 0, curInt;
	double runDoubleSum = 0.0, curDouble;

	if(!this->inPipe->Remove(rec)){
		this->outPipe->ShutDown();
		return ;
	}
	
	do{
		curAtt.myType = this->computeMe->Apply(*rec,curInt,curDouble);
		if(curAtt.myType==Int){
			runIntSum += curInt;
		}else if(curAtt.myType==Double){
			runDoubleSum += curDouble;
		}
	}while(this->inPipe->Remove(rec));
	Schema *sumResSchema = new Schema("",1,&curAtt);

	if(curAtt.myType == Int){
		rec->ComposeRecord(sumResSchema,(to_string(runIntSum)+'|').c_str());
	}else if(curAtt.myType == Double){
		rec->ComposeRecord(sumResSchema,(to_string(runDoubleSum)+'|').c_str());
	}

	this->outPipe->Insert(rec);
	this->outPipe->ShutDown();

}

/*
This function gets all the records from the inpipe and returns the groupby of the given attributes as a result into an out pipe.
*/
void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

/**/
void GroupBy :: Start(){

}

/*
This function initializes all the variables needed to perform write to file operation and spawns the thread which performs write to file operation.
*/
void WriteOut :: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

/*
This function gets all the records from the inpipe and writes all the records to the specified file with the given schema.
*/
void WriteOut :: Start(){
	Record *rec = new Record();
	while(this->inPipe->Remove(rec)){
		rec->WriteToFile(mySchema,outFile);
	}
	fclose(outFile);
}
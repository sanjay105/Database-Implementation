#include "RelOp.h"

void RelationalOp::WaitUntilDone (){
	cout<<"RelationalOp:WaitUntilDone : START"<<endl;
	pthread_join(th,NULL);
	cout<<"RelationalOp:WaitUntilDone : END"<<endl;
}

void RelationalOp::Use_n_Pages(int runlen){
	runLen = runlen;
}

void *performOperation(void *obj){
	((RelationalOp *)obj)->Start();
	return NULL;
}


void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	cout<<"SelectFile:Run : START"<<endl;
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	pthread_create(&th,NULL,performOperation, (void *)this);
	cout<<"SelectFile:Run : END"<<endl;
}

void SelectFile::Start(){
	cout<<"SelectFile:Start : START"<<endl;
	ComparisonEngine comp;
	Record *rec = new Record();
	cout<<"SelectFile:Start : inFile : "<<this->inFile<<endl;
	// while(this->inFile->GetNext(*rec)){
	// 	cout<<"SelectFile:Start : Record : "<<rec->Size()<<endl;
	// 	if(comp.Compare(rec,this->literal,this->selOp)){
	// 		this->outPipe->Insert(rec);
	// 	}
	// }

	while(this->inFile->GetNext(*rec,*this->selOp,*this->literal)){
		this->outPipe->Insert(rec);
	}

	this->outPipe->ShutDown();
	cout<<"SelectFile:Start : END"<<endl;

}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

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

void Project :: Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

void Project :: Start(){
	Record *rec = new Record();
	while(this->inPipe->Remove(rec)){
		rec->Project(this->keepMe,this->numAttsOutput,this->numAttsInput);
		this->outPipe->Insert(rec);
	}
	this->outPipe->ShutDown();
}

void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

void Join :: Start(){

}

void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

void DuplicateRemoval :: Start(){

}

void Sum :: Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

void Sum :: Start(){

}

void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

void GroupBy :: Start(){

}

void WriteOut :: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;

	pthread_create(&th,NULL,performOperation, (void *) this);
}

void WriteOut :: Start(){

}
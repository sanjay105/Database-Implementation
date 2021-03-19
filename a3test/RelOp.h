#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"



class RelationalOp {
	public:
	pthread_t th;
	int runLen;
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () ;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) ;

	virtual void Start() = 0;
};

class SelectFile : public RelationalOp { 

	private:
	// pthread_t thread;
	// Record *buffer;
	DBFile *inFile; 
	Pipe *outPipe; 
	CNF *selOp; 
	Record *literal;

	public:

	SelectFile() {};
	~SelectFile() {};

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void Start();

};

class SelectPipe : public RelationalOp {
	private:
	Pipe *inPipe, *outPipe; 
	CNF *selOp; 
	Record *literal;

	public:
	SelectPipe() {};
	~SelectPipe() {};

	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void Start();

};
class Project : public RelationalOp { 
	private:
	Pipe *inPipe, *outPipe; 
	int *keepMe, numAttsInput; 
	int numAttsOutput;

	public:
	Project() {};
	~Project() {};

	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void Start();

};
class Join : public RelationalOp { 
	private:
	Pipe *inPipeL, *inPipeR, *outPipe; 
	CNF *selOp; 
	Record *literal;

	public:
	Join() {};
	~Join() {};

	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void Start();
	
};
class DuplicateRemoval : public RelationalOp {
	private:
	Pipe *inPipe, *outPipe; 
	Schema *mySchema;

	public:
	DuplicateRemoval() {};
	~DuplicateRemoval() {};

	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void Start();
	
};
class Sum : public RelationalOp {
	private:
	Pipe *inPipe, *outPipe;
	Function *computeMe;

	public:
	Sum() {};
	~Sum() {};

	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void Start();

};
class GroupBy : public RelationalOp {
	private:
	Pipe *inPipe, *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;

	public:
	GroupBy() {};
	~GroupBy() {};

	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void Start();

};
class WriteOut : public RelationalOp {
	private:
	Pipe *inPipe; 
	FILE *outFile; 
	Schema *mySchema;

	public:
	// WriteOut() {};
	~WriteOut() {};
	
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void Start();

};
#endif

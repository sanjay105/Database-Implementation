#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 
struct SortInfo{
	OrderMaker *o;
	int runlen;
};

class BaseDBFile{
	protected:
		off_t pIndex;
		bool writeMode;
		char *fpath;
		Page *tempPage;
		File *curFile;
		bool isFileCreated;
		bool isFileOpened;
	public:
		virtual int Create (const char *fpath) = 0;
		virtual int Open (const char *fpath) = 0;
		virtual int Close () = 0;

		virtual void Load (Schema &myschema, const char *loadpath) = 0;

		virtual void MoveFirst () = 0;
		virtual void Add (Record &addme) = 0;
		virtual int GetNext (Record &fetchme) = 0;
		virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;

		// virtual ~BaseDBFile();
};

class HeapDBFile : public BaseDBFile{
	public:
		HeapDBFile();

		int Create (const char *fpath);
		int Open (const char *fpath);
		int Close ();

		void Load (Schema &myschema, const char *loadpath);

		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);

		~HeapDBFile();
};

class SortedDBFile : public BaseDBFile{
		int runlen;
		int bufSize;
		Pipe *inpipe,*outpipe;
		BigQ *bq;
		OrderMaker *o,*query;
	public:
		SortedDBFile(OrderMaker *o, int runlen);

		int Create (const char *fpath);
		int Open (const char *fpath);
		int Close ();

		void Load (Schema &myschema, const char *loadpath);

		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);

		void MergeBigQToFile();

		// Generate the query OrderMaker. Return 0 or 1,
		// representing generation success or failure.
		int QueryOrderGenerator (OrderMaker &query, OrderMaker &order, CNF &cnf);

		// Binary Search after query is successfully generated
		// return 0 or 1, representing found or not found
		int BinarySearch(Record &fetchme, CNF &cnf, Record &literal);

		// Get next function when query is not valid
		int GetNextSequential (Record &fetchme, CNF &cnf, Record &literal);

		// Get next function when query exist and valid
		int GetNextWithQuery (Record &fetchme, CNF &cnf, Record &literal);

		// Set up the Internal BigQ for Records to add
		void InitializeBigQ ();

		~SortedDBFile();

};

class DBFile {
	BaseDBFile *curDBObject;
public:
	DBFile (); 
	~DBFile();

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif

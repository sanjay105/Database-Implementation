#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparison.h"
#include <bits/stdc++.h>

using namespace std;


struct TPMMSParams{
	Pipe &in,&out;
	// vector<Pipe *> pipes;
	OrderMaker &sortorder;
	int runlen;
};


class RecordPQ{
	off_t offset;
	off_t endoffset;
	File *file;
	Page *tempPage;
	public:
	Record *record;
	RecordPQ(off_t offset,off_t endoffset,File *fp);
	bool GetNextRecord(Record *rec);
	~RecordPQ();
};

class OrderedCompare{
	OrderMaker *order;
	public:
		OrderedCompare(OrderMaker *orders);
		bool operator()(Record *a,Record *b);
		bool operator()(RecordPQ *a,RecordPQ *b);
		~OrderedCompare();
};

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif

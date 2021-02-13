#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;


struct TPPMSParams{
	Pipe &in;
	Pipe &out;
	OrderMaker &sortorder;
	int runlen;
};

class OrderedCompare{
	OrderMaker &order;
	public:
		OrderedCompare(OrderMaker &order);
		bool operator()(Record *a,Record *b);
		~OrderedCompare();
};

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif

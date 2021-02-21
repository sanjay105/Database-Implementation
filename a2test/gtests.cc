#include<bits/stdc++.h>
#include <gtest/gtest-death-test.h>
#include<gtest/gtest.h>

#include "test.h"
#include "BigQ.h"
#include "Comparison.h"


void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;

	Record temp;
	int counter = 0;

	DBFile dbfile;
	cout << " DBFile will be created at " << rel->path () << endl;
	dbfile.Create (rel->path(), heap, NULL);

	char tbl_path[100]; // construct path of the tpch flat text file
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
	cout << " tpch file will be loaded from " << tbl_path << endl;

	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.Close();
	dbfile.Open (rel->path ());
	cout << " producer: opened DBFile " << rel->path () << endl;
	dbfile.MoveFirst ();

	while (dbfile.GetNext (temp) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		myPipe->Insert (&temp);
	}

	dbfile.Close ();
	myPipe->ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
	return NULL;
}

void *consumer (void *arg) {
	
	testutil *t = (testutil *) arg;

	ComparisonEngine ceng;

	DBFile dbfile;
	char outfile[100];

	if (t->write) {
		sprintf (outfile, "%s.bigq", rel->path ());
		dbfile.Create (outfile, heap, NULL);
	}

	int err = 0;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;
	// cout<<"Inside Consumer Pipe Status "<<t->pipe->GetDone()<<endl;
	while (t->pipe->Remove (&rec[i%2])) {
		// cout<<"Consumer: Inside While"<<endl;
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (ceng.Compare (prev, last, t->order) == 1) {
				err++;
			}
			if (t->write) {
				dbfile.Add (*prev);
			}
		}
		if (t->print) {
			last->Print (rel->schema ());
		}
		i++;
		// cout<<"i: "<<i<<endl;
	}

	cout << " consumer: removed " << i << " recs from the pipe\n";

	if (t->write) {
		if (last) {
			dbfile.Add (*last);
		}
		cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
		dbfile.Close ();
	}
	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
	if (err) {
		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
	}
	
	return NULL;
}

// Google Test for invalid run length.
TEST(BigQ_Test_RunLen, InValidRunLen)
{
    OrderMaker sortorder;
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);
    EXPECT_EXIT( BigQ bq (input, output, sortorder, -1), ::testing::ExitedWithCode(7), "runlen .*");
}

// Google Test for valid open pipe.
TEST(BigQ_Test_Pipe, ValidPipeOpen)
{
	Pipe p (100);
    ASSERT_EQ(0, p.GetDone() );
}

// Google Test for valid closed input pipe.
TEST(BigQ_Test_Pipe, InPipeClosed)
{
    OrderMaker sortorder;
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
    Pipe output (buffsz);
    input.ShutDown();
    EXPECT_EXIT( BigQ bq (input, output, sortorder, 3), ::testing::ExitedWithCode(7), "in pipe is closed");
}

// Google Test for valid closed output pipe.
TEST(BigQ_Test_Pipe, OutPipeClosed)
{
    OrderMaker sortorder;
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
    Pipe output (buffsz);
    output.ShutDown();
    EXPECT_EXIT( BigQ bq (input, output, sortorder, 3), ::testing::ExitedWithCode(7), "out pipe is closed");
}



int main(int argc,char **argv){
    setup ();
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
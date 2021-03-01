#include "gtest/gtest.h"
#include "test.h"

// Running length initialization
int runlen = 0;

// This fucntion adds the mock data
int add_data (FILE *src, int noOfRecords, int &res) {
	DBFile dbfile;
	dbfile.Open (rel->path ());
	Record tempRecord;

	int processCnt = 0;
	while ((res = tempRecord.SuckNextRecord (rel->schema (), src)) && ++processCnt < noOfRecords) {
		dbfile.Add (tempRecord);
	}

	dbfile.Close ();
	return processCnt;
}

// Google Test for valid heap DB file create.
TEST(Heap_DBFile_Test, ValidCreate)
{
	DBFile dbfile;
	ASSERT_EQ(1, dbfile.Create(rel->path(), heap, NULL));
}

// Google Test for valid heap DB file open.
TEST(Heap_DBFile_Test, ValidOpen)
{
	DBFile dbfile;
	dbfile. Create(rel->path(),heap,NULL);
	ASSERT_EQ(1, dbfile.Open( rel->path()));
}

// Google Test for valid heap DB file close.
TEST(Heap_DBFile_Test, ValidClose)
{
	DBFile dbfile;
	dbfile.Create( rel->path(), heap, NULL);
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name());
	dbfile.Load (*(n->schema ()), tbl_path);
	ASSERT_EQ(1, dbfile.Close());
}

// Google Test for valid sorted DB file load.
TEST (Sorted_DBFile_Test, LoadTest) {
	OrderMaker o (rel->schema ());
	struct {OrderMaker *o; int l;} startup = {&o, runlen};
	DBFile dbfile;
	
	dbfile.Create (rel->path(), sorted, &startup);
	dbfile.Close ();

	srand48 (time (NULL));
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	
	FILE* tblfile = fopen (tbl_path, "r");
	int proc = 1, res = 1;
	while (proc && res) {
		proc = add_data (tblfile,lrand48()%(int)pow(1e3,2)+1000, res);
	}
	fclose (tblfile);
}

// Google Test for valid sorted DB file order examination.
TEST (Sorted_DBFile_Test, OrderTest) {
	OrderMaker o(rel->schema ());
	ComparisonEngine comp;
	DBFile dbfile;
	dbfile.Open (rel->path ());
	Record *rec = new Record;
	Record *prev = new Record;
	
	if (!dbfile.GetNext (*prev)) {
		while (dbfile.GetNext (*rec)) {
			ASSERT_NE (comp.Compare (prev, rec, &o), 1);
			prev->Copy (rec);
		}
	}
	dbfile.Close ();
	cleanup ();
}

// Google Tests Entry Point
int main (int argc, char **argv) {
	testing::InitGoogleTest(&argc, argv);
	setup ();
	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
	
	runlen = 2; // run length size
	int tableId = 3; // customer table

	rel = rel_ptr [tableId - 1];
	return RUN_ALL_TESTS ();
}
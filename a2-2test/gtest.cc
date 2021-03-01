#include "gtest/gtest.h"
#include "test.h"

int runlen = 0;

int add_data (FILE *src, int numrecs, int &res) {
	DBFile dbfile;
	dbfile.Open (rel->path ());
	Record temp;

	int proc = 0;
	int xx = 20000;
	while ((res = temp.SuckNextRecord (rel->schema (), src)) && ++proc < numrecs) {
		dbfile.Add (temp);
		// if (proc == xx) cerr << "\t ";
		// if (proc % xx == 0) cerr << ".";
	}

	dbfile.Close ();
	return proc;
}

TEST(Heap_DBFile_Test, ValidCreate)
{
	DBFile dbfile;
	ASSERT_EQ(1, dbfile.Create( rel->path(), heap, NULL));
}

TEST(Heap_DBFile_Test, ValidOpen)
{
	DBFile dbfile;
	dbfile. Create(rel->path(),heap,NULL);
	ASSERT_EQ(1, dbfile.Open( rel->path()) );
}

TEST(Heap_DBFile_Test, ValidClose)
{
	DBFile dbfile;
	dbfile.Create( rel->path(), heap, NULL);
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name());
	dbfile.Load (*(n->schema ()), tbl_path);
	ASSERT_EQ(1, dbfile.Close());
}

TEST (Sorted_DBFile_Test, LoadTest) {
	// randomly seperate records and load them into sorted files
	int count = 0;
	
	OrderMaker o (rel->schema ());
	
	struct {OrderMaker *o; int l;} startup = {&o, runlen};
	
	DBFile dbfile;
	// cout << "\t\n Start loading " << rel->path () << endl;
	
	dbfile.Create (rel->path(), sorted, &startup);
	dbfile.Close ();
	
	srand48 (time (NULL));
	
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	
	FILE* tblfile = fopen (tbl_path, "r");
	
	int proc = 1, res = 1, tot = 0;
	
	while (proc && res) {
		
		proc = add_data (tblfile,lrand48()%(int)pow(1e3,2)+1000, res);
		tot += proc;
		count++;
		// if (proc) 
		// 	cout << "\n\t Run " << count << " : added " << proc << " recs..so far " << tot << endl;
		
	}
	
	fclose (tblfile);
	// cout << "\n" << rel->path () << " created!" << endl;
	// cout << tot << " recs inserted" << endl;
	
}

TEST (Sorted_DBFile_Test, OrderTest) {
	// Examine the order inside the file after loading
	
	OrderMaker o(rel->schema ());
	ComparisonEngine comp;
	
	DBFile dbfile;
	dbfile.Open (rel->path ());
	// cout << "\t\n start to check the order of " << rel->path () << " . \t\n";		
	
	Record *rec = new Record;
	Record *prev = new Record;
	
	if (!dbfile.GetNext (*prev)) {
		
		while (dbfile.GetNext (*rec)) {
			
			ASSERT_NE (comp.Compare (prev, rec, &o), 1);
			prev->Copy (rec);
		
		}
		
	}
	
	dbfile.Close ();
	// cout << "\t\n done!" << endl;
	
	cleanup ();
	
}


int main (int argc, char **argv) {
	
	testing::InitGoogleTest(&argc, argv);
	
	setup ();
	
	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
	
	// while (runlen < 1) {
	// 	cout << "\t\n specify runlength:\n\t ";
	// 	cin >> runlen;
	// }
	runlen = 2;
	int findx = 3;
	// while (findx < 1 || findx > 8) {
	// 	cout << "\n select table: \n";
	// 	cout << "\t 1. nation \n";
	// 	cout << "\t 2. region \n";
	// 	cout << "\t 3. customer \n";
	// 	cout << "\t 4. part \n";
	// 	cout << "\t 5. partsupp \n";
	// 	cout << "\t 6. supplier \n";
	// 	cout << "\t 7. orders \n";
	// 	cout << "\t 8. lineitem \n \t ";
	// 	cin >> findx;
	// }
	rel = rel_ptr [findx - 1];
	return RUN_ALL_TESTS ();
	
}
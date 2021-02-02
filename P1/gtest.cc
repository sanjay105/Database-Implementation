#include<bits/stdc++.h>
#include <gtest/gtest-death-test.h>
#include<gtest/gtest.h>

#include "test.h"

// make sure that the file path/dir information below is correct
const char *dbfile_dir = ""; // dir where binary heap files should be stored
const char *tpch_dir ="/home/sanjay/Documents/Database-Implementation/git/tpch-dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
const char *catalog_path = "catalog"; // full path of the catalog file

// Google Test for creation of file scenario with valid parameters
TEST(DBFile_Test_Create, ValidCreate)
{
	DBFile dbfile;
	ASSERT_EQ(1, dbfile.Create( n->path(), heap, NULL));
}

// Google Test for creation of file scenario with invalid file path
 TEST(DBFile_Test_Create, InvalidCreatePath)
 {
     DBFile dbfile;
     EXPECT_EXIT( dbfile.Create( "", heap, NULL), ::testing::ExitedWithCode(1), "BAD.*");
 }

// Google Test for creation of file scenario with invalid file type
 TEST(DBFile_Test_Create, InvalidCreateFileType)
 {
     DBFile dbfile;
     EXPECT_EXIT( dbfile.Create( n->path(), sorted, NULL), ::testing::ExitedWithCode(2), "BAD.*");
 }

// Google Test for opening the file scenario with valid parameters
TEST(DBFile_Test_Open, ValidOpen)
{
	DBFile dbfile;
	ASSERT_EQ(1, dbfile.Open( r->path()) );
}

// Google Test for opening the file scenario with invalid/non-existant file path
TEST(DBFile_Test_Open, InvalidOpen)
{
	DBFile dbfile;
	EXPECT_EXIT( dbfile.Open("file_not_found.bin"), ::testing::ExitedWithCode(1), "BAD.*");
}

// Google Test for closing the file (nation.tbl) scenario with valid parameters
TEST(DBFile_Test_Close, ValidClose)
{
	DBFile dbfile;
	dbfile.Create( n->path(), heap, NULL);
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name());
	dbfile.Load (*(n->schema ()), tbl_path);
	ASSERT_EQ(1, dbfile.Close());
}

// Google Test for closing the file (non-existant) scenario.
TEST(DBFile_Test_Close, InvalidClose)
{
	DBFile dbfile;
	ASSERT_EQ(0, dbfile.Close());
}



int main(int argc,char **argv){
    setup (catalog_path, dbfile_dir, tpch_dir);
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
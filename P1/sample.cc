#include <iostream>
#include "DBFile.h"
#include "test.h"

// make sure that the file path/dir information below is correct
const char *dbfile_dir = ""; // dir where binary heap files should be stored
const char *tpch_dir ="/home/sanjay/Documents/Database-Implementation/git/tpch-dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
const char *catalog_path = "catalog"; // full path of the catalog file

using namespace std;

relation *rel;


int main(){
    setup (catalog_path, dbfile_dir, tpch_dir);
    DBFile dbfile;
    // dbfile.Create (rel->path(), sorted, NULL);
    dbfile.Create (rel->path(), heap, NULL);

}
#include "Schema.h"
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>

int Schema :: Find (char *attName) {

	for (int i = 0; i < numAtts; i++) {
		// cout<<"Attr: "<<myAtts[i].name<<" AttName: "<<attName<<endl;
		if (!strcmp (attName, myAtts[i].name)) {
			return i;
		}
	}

	// if we made it here, the attribute was not found
	return -1;
}

Type Schema :: FindType (char *attName) {

	for (int i = 0; i < numAtts; i++) {
		if (!strcmp (attName, myAtts[i].name)) {
			return myAtts[i].myType;
		}
	}

	// if we made it here, the attribute was not found
	return Int;
}

int Schema :: GetNumAtts () {
	return numAtts;
}

Attribute *Schema :: GetAtts () {
	return myAtts;
}

Schema :: Schema () : fileName (NULL), numAtts (0), myAtts (NULL) {}

Schema :: Schema (char *fpath, int num_atts, Attribute *atts) {
	// fileName = strdup (fpath);
	numAtts = num_atts;
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++ ) {
		if (atts[i].myType == Int) {
			myAtts[i].myType = Int;
		}
		else if (atts[i].myType == Double) {
			myAtts[i].myType = Double;
		}
		else if (atts[i].myType == String) {
			myAtts[i].myType = String;
		} 
		else {
			cout << "Bad attribute type for " << atts[i].myType << "\n";
			delete [] myAtts;
			exit (1);
		}
		myAtts[i].name = strdup (atts[i].name);
	}
}

Schema :: Schema (char *fName, char *relName) {

	FILE *foo = fopen (fName, "r");
	
	// this is enough space to hold any tokens
	char space[200];

	fscanf (foo, "%s", space);
	int totscans = 1;

	// see if the file starts with the correct keyword
	if (strcmp (space, "BEGIN")) {
		cout << "Unfortunately, this does not seem to be a schema file.\n";
		exit (1);
	}	
		
	while (1) {

		// check to see if this is the one we want
		fscanf (foo, "%s", space);
		totscans++;
		if (strcmp (space, relName)) {

			// it is not, so suck up everything to past the BEGIN
			while (1) {

				// suck up another token
				if (fscanf (foo, "%s", space) == EOF) {
					cerr << "Could not find the schema for the specified relation.\n";
					exit (1);
				}

				totscans++;
				if (!strcmp (space, "BEGIN")) {
					break;
				}
			}

		// otherwise, got the correct file!!
		} else {
			break;
		}
	}

	// suck in the file name
	fscanf (foo, "%s", space);
	totscans++;
	fileName = strdup (space);

	// count the number of attributes specified
	numAtts = 0;
	while (1) {
		fscanf (foo, "%s", space);
		if (!strcmp (space, "END")) {
			break;		
		} else {
			fscanf (foo, "%s", space);
			numAtts++;
		}
	}

	// now actually load up the schema
	fclose (foo);
	foo = fopen (fName, "r");

	// go past any un-needed info
	for (int i = 0; i < totscans; i++) {
		fscanf (foo, "%s", space);
	}

	// and load up the schema
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++ ) {

		// read in the attribute name
		fscanf (foo, "%s", space);	
		myAtts[i].name = strdup (space);

		// read in the attribute type
		fscanf (foo, "%s", space);
		if (!strcmp (space, "Int")) {
			myAtts[i].myType = Int;
		} else if (!strcmp (space, "Double")) {
			myAtts[i].myType = Double;
		} else if (!strcmp (space, "String")) {
			myAtts[i].myType = String;
		} else {
			cout << "Bad attribute type for " << myAtts[i].name << "\n";
			exit (1);
		}
	}

	fclose (foo);
}

Schema::Schema (const Schema& s) : fileName(0), myAtts(0) {
	// cout<<"Schema:: Copy Constructor START filename : "<<s.fileName<<endl;
	if (s.fileName) {
		
		fileName = strdup(s.fileName);
		
	}
	
	numAtts = s.numAtts;
	myAtts = new Attribute[numAtts];
	
	for (int i = 0; i < numAtts; i++ ) {
		
		myAtts[i] = s.myAtts[i];
		myAtts[i].name = strdup(myAtts[i].name);
		
	}
	// cout<<"Schema:: Copy Constructor END"<<endl;
	
}

Schema& Schema::operator= (const Schema& s) {
	
	if (s.fileName) {
		
		fileName = strdup(s.fileName);
		
	}
	
	numAtts = s.numAtts;
	myAtts = new Attribute[numAtts];
	
	for (int i = 0; i < numAtts; i++ ) {
		
		myAtts[i] = s.myAtts[i];
		myAtts[i].name = strdup (myAtts[i].name);
		
	}
	
	return *this;
}

void Schema :: Print () {
	
	string typeName;
	
	for (int i = 0; i < numAtts; i++) {
		
		switch (myAtts[i].myType) {
			
			case Int :
				typeName = string ("Int");
				break;
			
			case Double :
				typeName = string ("Double");
				break;
				
			case String : 
				typeName = string ("String");
				break;
			
			default :// should never come here!!!!!
				cout << "Wrong Type! " << myAtts[i].myType << endl;
			
		}
		
		cout << myAtts[i].name << " : " <<  typeName << endl;
		
	}
	
}

void Schema :: Reseat (string prefix) {
	
	for (int i = 0; i < numAtts; i++) {
		
		string oldName (myAtts[i].name);
		free (myAtts[i].name);
		string newName (prefix + "." + oldName);
		myAtts[i].name = strdup (newName.c_str());
		
    }
	
}

void Schema :: GroupBySchema (Schema s, bool returnInt) {
	
	numAtts = s.GetNumAtts () + 1;
	
	if (myAtts) {
		
		delete[] myAtts;
	
	}
	
	myAtts = new Attribute[numAtts];
	
	Attribute atts[2] = {{"sum", Int}, {"sum", Double}};
	
	if (returnInt) {
		
		myAtts[0] = atts[0];
		
	} else {
		
		myAtts[0] = atts[1];
		
	}
	
	for (int i = 0; i < s.numAtts; i++) {
		
		myAtts[i + 1] = s.myAtts[i];
		
	}
	
}

void Schema :: ProjectSchema (Schema s, vector<string> names, vector<int> &attsToKeep) {
	
	numAtts = names.size ();
	
	if (myAtts) {
		
		delete[] myAtts;
	
	}
	
	myAtts = new Attribute[numAtts];
	
	for (int i = 0; i < numAtts; i++) {
		
		attsToKeep.push_back (s.Find (strdup (names[i].c_str ())));
		
		myAtts[i] = s.myAtts[attsToKeep.back ()];
		
	}
	
}

void Schema :: JoinSchema (Schema left, Schema right) {
	
	numAtts = left.numAtts + right.numAtts;
	
	if (myAtts) {
		
		delete[] myAtts;
	
	}
	
	myAtts = new Attribute[numAtts];
	
	for (int i = 0; i < left.numAtts; i++) {
		
		myAtts[i] = left.myAtts[i];
		
	}
	
	for (int i = 0; i < right.numAtts; i++) {
		
		myAtts[left.numAtts + i] = right.myAtts[i];
		
	}
	
}

Schema :: ~Schema () {
	delete [] myAtts;
	myAtts = 0;
}


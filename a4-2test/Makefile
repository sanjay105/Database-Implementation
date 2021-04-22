
CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

a42.out: Schema.o Record.o Comparison.o ComparisonEngine.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o  y.tab.o lex.yy.o main.o
	$(CC) -o a42.out Schema.o Record.o Comparison.o ComparisonEngine.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o main.o -lfl -lpthread


gtest.out: Schema.o Record.o Comparison.o ComparisonEngine.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o  y.tab.o lex.yy.o 
	$(CC) -o gtest.out Schema.o Record.o Comparison.o ComparisonEngine.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o gtests.cc -lfl -lpthread -lgtest
	
main.o : main.cc
	$(CC) -g -c main.cc
	
# a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o Statistics.o y.tab.o lex.yy.o test.o
# 	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o Statistics.o y.tab.o lex.yy.o test.o -lfl -lpthread	

# test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
# 	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lfl -lpthread
	

Function.o: Function.cc
	$(CC) -g -c Function.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc


y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h

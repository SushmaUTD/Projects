A1 = LOAD '/test/review.csv' AS line;
B1 = FOREACH A1 GENERATE FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) AS (b1,b2,b3,b4);
A2 = LOAD '/test/business.csv' AS line;
B2 = FOREACH A2 GENERATE FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)')) AS (c1,c2,c3);
C3 = COGROUP B1 BY b3,B2 BY c1;
C4 = limit C3 5;
dump C4;

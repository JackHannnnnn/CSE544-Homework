/* CSE 544 Homework 1: Data Analytics Pipeline

- Objectives:
	To get familiar with the main components of the data analytic pipeline: schema design, 
	data acquisition, data transformation, querying, and visualizing.
- Assignment tools:
	postgres, excel (or some other tool for visualization)
*/


-- 1. Schema Design

-- Create a Database
CREATEDB dblp
PSQL dblp

-- Create the Publication Schema in SQL
CREATE TABLE Author (
		AuthorID INT NOT NULL CONSTRAINT PK_Author PRIMARY KEY,
		Name TEXT NOT NULL,
		Homepage TEXT
		);

CREATE TABLE Publication (
		PubID INT NOT NULL CONSTRAINT PK_Publication PRIMARY KEY,
		PubKey TEXT NOT NULL,
		Title TEXT,
		Year TEXT
		);
		
CREATE TABLE Authored (
		AuthorID INT NOT NULL CONSTRAINT FK_Author REFERENCES Author (AuthorID),
		PubID INT NOT NULL CONSTRAINT FK_Publication REFERENCES Publication (PubID),
		CONSTRAINT PK_Authored PRIMARY KEY (AuthorID, PubID)
		);
		
CREATE TABLE Article (
		PubID INT UNIQUE NOT NULL CONSTRAINT FK_Article REFERENCES Publication (PubID),
		Journal TEXT,
		Month TEXT,
		Volume TEXT,
		Number TEXT,
		PRIMARY KEY (PubID)
		);
		
CREATE TABLE Book (
		PubID INT UNIQUE NOT NULL CONSTRAINT FK_Book REFERENCES Publication (PubID),
		Publisher TEXT,
		ISBN TEXT,
		PRIMARY KEY (PubID)
		);
		
CREATE TABLE InCollection (
		PubID INT UNIQUE NOT NULL CONSTRAINT FK_InCollection REFERENCES Publication (PubID),
		BookTitle TEXT,
		Publisher TEXT,
		ISBN TEXT,
		PRIMARY KEY (PubID)
		);
		
CREATE TABLE InProceedings (
		PubID INT UNIQUE NOT NULL CONSTRAINT FK_InProceedings REFERENCES Publication (PubID),
		BookTitle TEXT,
		Editor TEXT,
		PRIMARY KEY (PubID)
		);
		
-- 2. Data Acquisition

-- Import DBLP into postgres
---Step 1: Download dblp.dtd and dblp.xml (300MB gziped as of Oct 2015) from http://dblp.uni-trier.de/xml/. 
---Step 2: Run wrapper.py
---Step 3: Run createRawSchema.sql

-- Write queries
-- q1. For each type of publication, count the total number of publications of that type.
SELECT p AS PublicationType, COUNT(k)
	FROM Pub
	GROUP BY p;
	
/*
Result:

 PublicationType |  count  
-----------------+---------
 www             | 1791664
 incollection    |   39575
 article         | 1525016
 phdthesis       |   32717
 inproceedings   | 1874143
 book            |   12776
 proceedings     |   31928
 mastersthesis   |       9
*/

-- q2. Find the fields that occur in all publications types.
SELECT f.p, COUNT(DISTINCT p.p)
	FROM Pub p INNER JOIN Field f ON p.k = f.k
	GROUP BY f.p
	HAVING COUNT(DISTINCT p.p) >= 8;
	
/*
Result:

   p   | count 
-------+-------
 ee    |     8
 title |     8
 url   |     8
 year  |     8
*/

-- q3. Speed up by creating appropriate indexes
CREATE INDEX PubKey ON Pub(k);
CREATE INDEX PubP ON Pub(p);

CREATE INDEX FieldKey ON Field(k);
CREATE INDEX FieldP ON Field(p);
CREATE INDEX FieldV ON Field(v);



-- 3. Data Transformation

-- Transform the DBLP data from RawSchema to PubSchema. 
CREATE TABLE tempAuthor (
		PubKey TEXT NOT NULL,
		Name TEXT
		);

CREATE TABLE tempTitle (
		PubKey TEXT NOT NULL UNIQUE,
		Title TEXT
		);
		
CREATE TABLE tempYear (
		PubKey TEXT NOT NULL UNIQUE,
		Year TEXT
		);

CREATE TABLE tempJournal (
		PubKey TEXT NOT NULL UNIQUE,
		Journal TEXT
		);

CREATE TABLE tempPublisher (
		PubKey TEXT NOT NULL UNIQUE,
		Publisher TEXT
		);

CREATE TABLE tempMonth (
		PubKey TEXT NOT NULL UNIQUE,
		Month TEXT
		);
		
CREATE TABLE tempVolume (
		PubKey TEXT NOT NULL UNIQUE,
		Volume TEXT
		);
	
CREATE TABLE tempNumber (
		PubKey TEXT NOT NULL UNIQUE,
		Number TEXT
		);

CREATE TABLE tempISBN (
		PubKey TEXT NOT NULL UNIQUE,
		ISBN TEXT
		);

CREATE TABLE tempBookTitle (
		PubKey TEXT NOT NULL UNIQUE,
		BookTitle TEXT
		);
		
CREATE TABLE tempEditor (
		PubKey TEXT NOT NULL UNIQUE,
		Editor TEXT
		);

INSERT INTO tempAuthor (SELECT k, v FROM Field WHERE p = 'author');
	
WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'title')
INSERT INTO tempTitle (SELECT k, v FROM tmp WHERE r = 1);

WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'year')
INSERT INTO tempYear (SELECT k, v FROM tmp WHERE r = 1);

WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'journal')
INSERT INTO tempJournal (SELECT k, v FROM tmp WHERE r = 1);

WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'publisher')
INSERT INTO tempPublisher (SELECT k, v FROM tmp WHERE r = 1);

WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'month')
INSERT INTO tempMonth (SELECT k, v FROM tmp WHERE r = 1);

WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'volume')
INSERT INTO tempVolume (SELECT k, v FROM tmp WHERE r = 1);
	
WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'number')
INSERT INTO tempNumber (SELECT k, v FROM tmp WHERE r = 1);

WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'isbn')
INSERT INTO tempISBN (SELECT k, v FROM tmp WHERE r = 1);

WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'booktitle')
INSERT INTO tempBookTitle (SELECT k, v FROM tmp WHERE r = 1);

WITH tmp AS (SELECT ROW_NUMBER() OVER (PARTITION BY k) AS r, k, v
				FROM Field WHERE p = 'editor')
INSERT INTO tempEditor (SELECT k, v FROM tmp WHERE r = 1);


				
CREATE SEQUENCE seqAuthor;
CREATE SEQUENCE seqPublication;

CREATE TABLE tempHomepage (
	Name TEXT NOT NULL UNIQUE,
	Homepage TEXT
	);

WITH tmp AS (SELECT row_number() over (partition BY ta.Name) AS r, ta.Name, f2.v AS hp
				FROM tempAuthor ta INNER JOIN Field f1 ON ta.PubKey = f1.k
						INNER JOIN Field f2 ON ta.PubKey = f2.k
				WHERE f1.p = 'title' AND f1.v = 'Home Page' AND f2.p = 'url');
INSERT INTO tempHomepage (SELECT Name, hp FROM tmp WHERE tmp.r = 1);
		
INSERT INTO Author (
	SELECT NEXTVAL('seqAuthor'), ta.Name, th.Homepage
		FROM (SELECT DISTINCT Name FROM tempAuthor) AS ta 
				LEFT OUTER JOIN tempHomepage AS th ON ta.Name = th.Name
	);
	
INSERT INTO Publication (
	SELECT NEXTVAL('seqPublication'), p.k, tt.Title, ty.Year
		FROM Pub AS p 
			LEFT OUTER JOIN tempTitle AS tt ON p.k = tt.PubKey
			LEFT OUTER JOIN tempYear AS ty ON p.k = ty.PubKey
		WHERE p.p in ('article', 'book', 'inproceedings', 'incollection')
	);
	
DROP TABLE tempHomepage;
DROP SEQUENCE seqAuthor;	
DROP SEQUENCE seqPublication;


INSERT INTO Authored (
	SELECT DISTINCT a.AuthorID, p.PubID
		FROM tempAuthor AS ta 
			INNER JOIN Author AS a on ta.Name = a.Name
			INNER JOIN Publication AS p ON ta.PubKey = p.PubKey
	);

INSERT INTO Article (
	SELECT p.PubID, tj.Journal, tm.Month, tv.Volume, tn.Number
		FROM Publication AS p
			LEFT OUTER JOIN tempJournal AS tj ON p.PubKey = tj.PubKey
			LEFT OUTER JOIN tempMonth AS tm ON p.PubKey = tm.PubKey
			LEFT OUTER JOIN tempVolume AS tv ON p.PubKey = tv.PubKey
			LEFT OUTER JOIN tempNumber AS tn ON p.PubKey = tn.PubKey
		WHERE EXISTS (
			SELECT * FROM Pub
			WHERE Pub.k = p.PubKey and Pub.p = 'article'
			)
	);	

INSERT INTO Book (
	SELECT p.PubID, tp.Publisher, ti.ISBN
		FROM Publication AS p
			LEFT OUTER JOIN tempPublisher AS tp ON p.PubKey = tp.PubKey
			LEFT OUTER JOIN tempISBN AS ti ON p.PubKey = ti.PubKey
		WHERE EXISTS (
			SELECT * FROM Pub
			WHERE Pub.k = p.PubKey and Pub.p = 'book'
			)
	);

INSERT INTO InCollection (
	SELECT p.PubID, tb.BookTitle, tp.Publisher, ti.ISBN
		FROM Publication AS p
			LEFT OUTER JOIN tempBookTitle AS tb ON p.PubKey = tb.PubKey
			LEFT OUTER JOIN tempPublisher AS tp ON p.PubKey = tp.PubKey
			LEFT OUTER JOIN tempISBN AS ti ON p.PubKey = ti.PubKey
		WHERE EXISTS (
			SELECT * FROM Pub
			WHERE Pub.k = p.PubKey and Pub.p = 'incollection'
			)
	);
	
INSERT INTO InProceedings (
	SELECT p.PubID, tb.BookTitle, te.Editor
		FROM Publication AS p
			LEFT OUTER JOIN tempBookTitle AS tb ON p.PubKey = tb.PubKey
			LEFT OUTER JOIN tempEditor AS te ON p.PubKey = te.PubKey
		WHERE EXISTS (
			SELECT * FROM Pub
			WHERE Pub.k = p.PubKey and Pub.p = 'inproceedings'
			)
	);


DROP TABLE tempAuthor;
DROP TABLE tempTitle;
DROP TABLE tempYear;
DROP TABLE tempJournal;
DROP TABLE tempPublisher;
DROP TABLE tempMonth;
DROP TABLE tempVolume;
DROP TABLE tempNumber;
DROP TABLE tempISBN;
DROP TABLE tempBookTitle;
DROP TABLE tempEditor;

			
-- 4. Queries	
-- q1. Find the top 20 authors with the largest number of publications. Runtime: under 10s.
WITH tmp AS (SELECT AuthorID, COUNT(PubID) AS NumPublications
				FROM Authored
				GROUP BY AuthorID
				ORDER BY NumPublications DESC
				LIMIT 20)
SELECT a.AuthorID, Name, NumPublications
	FROM Author AS a INNER JOIN tmp ON a.AuthorID = tmp.AuthorID;

/*
Result:

 authorid |         name         | numpublications 
----------+----------------------+-----------------
   567942 | H. Vincent Poor      |            1310
  1680832 | Wei Wang             |            1006
  1681020 | Wei Zhang            |            1005
  1273436 | Philip S. Yu         |             981
  1685741 | Wen Gao 0001         |             981
   915820 | Lajos Hanzo          |             924
  1132699 | Mohamed-Slim Alouini |             918
  1774351 | Yu Zhang             |             896
  1594089 | Thomas S. Huang      |             895
   753547 | Jing Li              |             885
   936207 | Li Zhang             |             858
   819585 | Jun Liu              |             855
   263156 | Chin-Chen Chang      |             852
  1572488 | Tao Li               |             850
  1737608 | Yang Liu             |             839
   419810 | Elisa Bertino        |             835
   570539 | Hai Jin              |             813
  1701473 | Witold Pedrycz       |             806
   188105 | Bin Li               |             804
  1714641 | Xiaodong Wang        |             801
*/

-- q2. Find the top 20 authors with the largest number of publications in STOC. 
CREATE MATERIALIZED VIEW STOC AS
	SELECT DISTINCT f.k AS PubKey
		FROM Field f
		WHERE (f.p = 'booktitle' AND f.v LIKE '%STOC%') OR
			  (f.p = 'crossref' AND f.v LIKE '%STOC%') OR
			  (f.p = 'title' AND f.v LIKE '%Symposium on Theory of Computing%');

WITH tmp AS (SELECT ad.AuthorID, COUNT(ad.PubID) AS NumPublications
				FROM Authored AS ad
					INNER JOIN Publication AS p ON ad.PubID = p.PubID
					INNER JOIN STOC AS s ON p.PubKey = s.PubKey
				GROUP BY ad.AuthorID
				ORDER BY NumPublications DESC
				LIMIT 20)
SELECT a.AuthorID, Name, NumPublications
	FROM Author AS a INNER JOIN tmp ON a.AuthorID = tmp.AuthorID;



/*
Result:

 authorid |           name            | numpublications 
----------+---------------------------+-----------------
   152092 | Avi Wigderson             |              56
  1358358 | Robert Endre Tarjan       |              33
  1140744 | Moni Naor                 |              28
  1316435 | Rafail Ostrovsky          |              27
  1327138 | Ran Raz                   |              27
  1635292 | Uriel Feige               |              27
   488586 | Frank Thomson Leighton    |              25
  1111501 | Mihalis Yannakakis        |              25
  1196386 | Noam Nisan                |              25
  1206147 | Oded Goldreich            |              24
  1286001 | Prabhakar Raghavan        |              24
   284471 | Christos H. Papadimitriou |              24
  1429752 | Santosh Vempala           |              22
  1415885 | Salil P. Vadhan           |              22
   169799 | Baruch Awerbuch           |              21
   450555 | Eyal Kushilevitz          |              21
  1144231 | Moses Charikar            |              21
  1115605 | Miklós Ajtai              |              21
   418665 | Eli Upfal                 |              20
  1115259 | Mikkel Thorup             |              20
*/

CREATE MATERIALIZED VIEW PODS AS
	SELECT DISTINCT f.k AS PubKey
		FROM Field f
		WHERE (f.p = 'booktitle' AND f.v LIKE '%PODS%') OR
			  (f.p = 'cdrom' AND f.v LIKE '%PODS%') OR
			  (f.p = 'note' AND f.v LIKE '%PODS%') OR
			  (f.p = 'title' AND f.v LIKE '%PODS%') OR
			  (f.p = 'title' AND f.v LIKE '%Symposium on Principles of Database Systems%');

WITH tmp AS (SELECT ad.AuthorID, COUNT(ad.PubID) AS NumPublications
				FROM Authored AS ad
					INNER JOIN Publication AS p ON ad.PubID = p.PubID
					INNER JOIN PODS AS po ON p.PubKey = po.PubKey
				GROUP BY ad.AuthorID
				ORDER BY NumPublications DESC
				LIMIT 20)
SELECT a.AuthorID, Name, NumPublications
	FROM Author AS a INNER JOIN tmp ON a.AuthorID = tmp.AuthorID;



/*
Result:

 authorid |           name            | numpublications 
----------+---------------------------+-----------------
   933026 | Leonid Libkin             |              34
  1747261 | Yehoshua Sagiv            |              31
  1654033 | Victor Vianu              |              30
  1449160 | Serge Abiteboul           |              30
  1144488 | Moshe Y. Vardi            |              29
   518019 | Georg Gottlob             |              29
   325834 | Dan Suciu                 |              25
  1276901 | Phokion G. Kolaitis       |              25
  1377262 | Ronald Fagin              |              22
  1621843 | Tova Milo                 |              22
   284471 | Christos H. Papadimitriou |              21
   704107 | Jan Van den Bussche       |              20
   380772 | Dirk Van Gucht            |              19
   487854 | Frank Neven               |              18
  1688821 | Wenfei Fan                |              17
  1317539 | Raghu Ramakrishnan        |              16
   723970 | Jeffrey D. Ullman         |              16
   236855 | Catriel Beeri             |              15
   178273 | Benny Kimelfeld           |              15
  1090539 | Michael Benedikt          |              15
*/


CREATE MATERIALIZED VIEW SIGMOD AS
	SELECT DISTINCT f.k AS PubKey
		FROM Field f
		WHERE (f.p = 'booktitle' AND f.v LIKE '%SIGMOD%') OR
			  (f.p = 'cdrom' AND f.v LIKE '%SIGMOD%') OR
			  (f.p = 'journal' AND f.v LIKE '%SIGMOD%') OR
			  (f.p = 'note' AND f.v LIKE '%SIGMOD%') OR
			  (f.p = 'url' AND f.v LIKE '%SIGMOD%') OR
			  (f.p = 'title' AND f.v LIKE '%SIGMOD%') OR
			  (f.p = 'title' AND f.v LIKE '%Special Interest Group on Management of Data%');

WITH tmp AS (SELECT ad.AuthorID, COUNT(ad.PubID) AS NumPublications
				FROM Authored AS ad
					INNER JOIN Publication AS p ON ad.PubID = p.PubID
					INNER JOIN SIGMOD AS si ON p.PubKey = si.PubKey
				GROUP BY ad.AuthorID
				ORDER BY NumPublications DESC
				LIMIT 20)
SELECT a.AuthorID, Name, NumPublications
	FROM Author AS a INNER JOIN tmp ON a.AuthorID = tmp.AuthorID;



/*
Result:

 authorid |         name          | numpublications 
----------+-----------------------+-----------------
  1027219 | Marianne Winslett     |              82
  1101428 | Michael Stonebraker   |              72
  1350835 | Richard T. Snodgrass  |              70
  1095326 | Michael J. Franklin   |              61
  1095162 | Michael J. Carey      |              59
   349019 | David J. DeWitt       |              58
  1541259 | Surajit Chaudhuri     |              58
   724105 | Jeffrey F. Naughton   |              55
   381036 | Divesh Srivastava     |              55
   567855 | H. V. Jagadish        |              50
   592636 | Hector Garcia-Molina  |              48
   728739 | Jennifer Widom        |              47
   797288 | Joseph M. Hellerstein |              47
   746531 | Jiawei Han            |              46
   175208 | Beng Chin Ooi         |              44
  1317539 | Raghu Ramakrishnan    |              42
   768938 | Johannes Gehrke       |              41
  1271879 | Philip A. Bernstein   |              41
   872135 | Kenneth A. Ross       |              40
    76426 | Alon Y. Halevy        |              37
*/

-- q3. Find (a) all authors who published at least 10 SIGMOD papers but never published a 
--     PODS paper, and (b) all authors who published at least 5 PODS papers but never 
--     published a SIGMOD paper. Runtime: under 10s.

CREATE VIEW tmpPODS AS (SELECT ad.AuthorID, COUNT(ad.PubID) AS NumPublications
							FROM Authored AS ad
								INNER JOIN Publication AS p ON ad.PubID = p.PubID
								INNER JOIN PODS AS po ON p.PubKey = po.PubKey
							GROUP BY ad.AuthorID
							ORDER BY NumPublications DESC);

CREATE VIEW tmpSIGMOD AS (SELECT ad.AuthorID, COUNT(ad.PubID) AS NumPublications
				      		  FROM Authored AS ad
						    		INNER JOIN Publication AS p ON ad.PubID = p.PubID
						   		    INNER JOIN SIGMOD AS si ON p.PubKey = si.PubKey
					   		  GROUP BY ad.AuthorID
					  		  ORDER BY NumPublications DESC);
					   
SELECT ts.AuthorID, a.Name, ts.NumPublications
	FROM tmpPODS AS tp 
		FULL OUTER JOIN tmpSIGMOD AS ts ON tp.AuthorID = ts.AuthorID
		LEFT OUTER JOIN Author AS a ON ts.AuthorID = a.AuthorID
	WHERE tp.NumPublications IS NULL AND ts.NumPublications >= 10
	ORDER BY ts.NumPublications DESC;

/*
Result:

 authorid |           name           | numpublications 
----------+--------------------------+-----------------
   746531 | Jiawei Han               |              46
   387033 | Donald Kossmann          |              33
   749925 | Jim Gray                 |              30
   273886 | Christian S. Jensen      |              29
   343929 | David B. Lomet           |              28
   422695 | Elke A. Rundensteiner    |              27
   899350 | Krithi Ramamritham       |              27
   989472 | M. Tamer Özsu            |              26
   748418 | Jignesh M. Patel         |              25
    67495 | Alfons Kemper            |              25
   134813 | Arie Segev               |              25
   123321 | Anthony K. H. Tung       |              23
  1183297 | Nick Roussopoulos        |              22
   725615 | Jeffrey Xu Yu            |              22
  1646097 | Vanessa Braganholo       |              22
   921666 | Laura M. Haas            |              21
  1716088 | Xiaokui Xiao             |              21
   750267 | Jim Melton               |              21
   945473 | Ling Liu                 |              21
   876889 | Kevin Chen-Chuan Chang   |              21
   558852 | Guy M. Lohman            |              21
    85799 | AnHai Doan               |              20
    37475 | Ahmed K. Elmagarmid      |              20
  1516049 | Stefano Ceri             |              20
  1796389 | Zachary G. Ives          |              20
   850615 | Karl Aberer              |              20
   649027 | Ihab F. Ilyas            |              20
  1529577 | Stratos Idreos           |              19
   467820 | Feifei Li 0001           |              18
   139206 | Arnon Rosenthal          |              18
  1601549 | Tim Kraska               |              18
  1666779 | Volker Markl             |              18
  1489815 | Sihem Amer-Yahia         |              18
  1728240 | Xuemin Lin               |              18
   539167 | Goetz Graefe             |              18
   100757 | Andrew Eisenberg         |              18
  1511091 | Stanley B. Zdonik        |              18
   740874 | Jian Pei                 |              18
    82946 | Amit P. Sheth            |              17
   330541 | Daniel J. Abadi          |              17
   556168 | Guoliang Li              |              17
   147369 | Asuman Dogac             |              17
   206919 | Bruce G. Lindsay 0001    |              16
   400526 | E. F. Codd               |              15
   964676 | Luis Gravano             |              15
   216676 | C. J. Date               |              15
   715773 | Jayavel Shanmugasundaram |              14
  1242319 | Patrick Valduriez        |              14
   184344 | Betty Salzberg           |              14
   656577 | Ioana Manolescu          |              14
   816038 | Juliana Freire           |              14
   859979 | Kaushik Chakrabarti      |              14
   188046 | Bin Cui                  |              14
   878658 | Kevin S. Beyer           |              13
  1536225 | Suman Nath               |              13
  1719763 | Xifeng Yan               |              13
   297474 | Clement T. Yu            |              13
   510602 | Gao Cong                 |              13
   618794 | Hongjun Lu               |              13
   657618 | Ion Stoica               |              13
   820048 | Jun Yang 0001            |              13
  1146143 | Mourad Ouzzani           |              13
  1185160 | Nicolas Bruno            |              13
   743841 | Jianhua Feng             |              12
   715489 | Jayant Madhavan          |              12
  1298235 | Qiong Luo                |              12
   800766 | José A. Blakeley         |              12
  1714773 | Xiaofang Zhou            |              12
   433230 | Erhard Rahm              |              11
   300523 | Cong Yu                  |              11
  1720266 | Xin Luna Dong            |              11
  1716204 | Xiaolei Qian             |              11
  1070609 | Matthias Jarke           |              11
  1540341 | Sunita Sarawagi          |              11
   250936 | Chee Yong Chan           |              11
  1631937 | Ugur Çetintemel          |              11
  1513795 | Stefan Manegold          |              11
  1234476 | Paolo Papotti            |              11
   233841 | Carsten Binnig           |              11
   229638 | Carlos Ordonez           |              11
   745911 | Jianzhong Li             |              11
   761246 | Joachim Hammer           |              11
    80366 | Amihai Motro             |              11
   533025 | Gio Wiederhold           |              11
   435745 | Eric N. Hanson           |              11
   925147 | Lawrence A. Rowe         |              11
  1806650 | Zhenjie Zhang            |              10
    89439 | Anastassia Ailamaki      |              10
   112999 | Anisoara Nica            |              10
   129048 | Antonios Deligiannakis   |              10
   223052 | Calton Pu                |              10
   226913 | Carlo Curino             |              10
   581675 | Hans-Arno Jacobsen       |              10
   649867 | Il-Yeol Song             |              10
   694835 | James Cheng              |              10
   730259 | Jens Teubner             |              10
   755386 | Jingren Zhou             |              10
   838180 | K. Selçuk Candan         |              10
   909239 | Kyuseok Shim             |              10
  1001968 | Malú Castellanos         |              10
  1020783 | Margaret H. Eich         |              10
  1164964 | Nan Tang 0001            |              10
  1274882 | Philippe Bonnet          |              10
  1426431 | Sang-Won Lee             |              10
  1506987 | Sourav S. Bhowmick       |              10
  1583740 | Themis Palpanas          |              10
  1590787 | Thomas J. Cook           |              10
  1647795 | Vasilis Vassalos         |              10
  1705281 | Wook-Shin Han            |              10
(109 rows)
*/

SELECT tp.AuthorID, a.Name, tp.NumPublications
	FROM tmpPODS AS tp 
		FULL OUTER JOIN tmpSIGMOD AS ts ON tp.AuthorID = ts.AuthorID
		LEFT OUTER JOIN Author AS a ON tp.AuthorID = a.AuthorID
	WHERE tp.NumPublications >= 5 AND ts.NumPublications IS NULL
	ORDER BY tp.NumPublications DESC;

/*
Result:

 authorid |          name           | numpublications 
----------+-------------------------+-----------------
   478713 | Floris Geerts           |              11
   353147 | David P. Woodruff       |              10
   422684 | Eljas Soisalon-Soininen |               8
  1511649 | Stavros S. Cosmadakis   |               8
  1330690 | Rasmus Pagh             |               6
  1648387 | Vassos Hadzilacos       |               5
   849342 | Kari-Jouko Räihä        |               5
  1089305 | Michael A. Bender       |               5
  1165239 | Nancy A. Lynch          |               5
    47738 | Alan Nash               |               5
(10 rows)
*/


DROP VIEW tmpPODS;
DROP VIEW tmpSIGMOD;



-- q4. For each decade, compute the total number of publications in DBLP in that decade.

CREATE TABLE tempYear (
		Year INT,
		NumPublications INT
		);
INSERT INTO tempYear (
		SELECT CAST(Year AS INT), COUNT(PubKey)
			FROM Publication
			WHERE Year IS NOT NULL
			GROUP BY Year);

SELECT ty1.Year AS StartYear, SUM(ty2.NumPublications)
	FROM tempYear AS ty1, tempYear AS ty2
	WHERE ty1.Year <= ty2.Year AND 
		  ty2.Year < ty1.Year + 10 AND
		  ty1.Year <= 2008
	GROUP BY ty1.Year
	ORDER BY ty1.Year;

DROP TABLE tempYear;

/*
Result:

 startyear |   sum   
-----------+---------
      1936 |     113
      1937 |     132
      1938 |     127
      1939 |     132
      1940 |     139
      1941 |     152
      1942 |     156
      1943 |     175
      1944 |     279
      1945 |     396
      1946 |     508
      1947 |     625
      1948 |     837
      1949 |    1079
      1950 |    1511
      1951 |    1913
      1952 |    2514
      1953 |    3416
      1954 |    4099
      1955 |    4769
      1956 |    5585
      1957 |    6523
      1958 |    7730
      1959 |    9306
      1960 |   10491
      1961 |   11848
      1962 |   13592
      1963 |   15409
      1964 |   17659
      1965 |   20413
      1966 |   23130
      1967 |   26156
      1968 |   29434
      1969 |   32372
      1970 |   35841
      1971 |   40074
      1972 |   44309
      1973 |   49231
      1974 |   54744
      1975 |   61046
      1976 |   67638
      1977 |   76730
      1978 |   86110
      1979 |   97989
      1980 |  112014
      1981 |  127783
      1982 |  146291
      1983 |  167141
      1984 |  192932
      1985 |  222366
      1986 |  253086
      1987 |  284584
      1988 |  319476
      1989 |  357431
      1990 |  399376
      1991 |  447654
      1992 |  497701
      1993 |  553908
      1994 |  620456
      1995 |  702297
      1996 |  804245
      1997 |  916722
      1998 | 1040025
      1999 | 1171484
      2000 | 1312727
      2001 | 1450477
      2002 | 1599328
      2003 | 1748477
      2004 | 1892261
      2005 | 2022995
      2006 | 2135417
      2007 | 2139526
      2008 | 1967738
(73 rows)
*/

-- q5. Find the top 20 most collaborative authors.

CREATE TABLE CoAuthor (
		ID1 INT,
		ID2 INT
		);
INSERT INTO CoAuthor (SELECT a1.AuthorID AS ID1, a2.AuthorID AS ID2
						  FROM Authored AS a1 INNER JOIN Authored AS a2 ON a1.PubID = a2.PubID
						  WHERE a1.AuthorID <> a2.AuthorID);
							
SELECT ID1, COUNT(DISTINCT ID2) AS NumCollaborators
	FROM CoAuthor
	GROUP BY ID1
	ORDER BY NumCollaborators DESC
	LIMIT 20;

/*
Result:

   id1   | numcollaborators 
---------+------------------
 1680832 |             2190
 1681020 |             1922
 1680500 |             1746
 1737608 |             1646
  753547 |             1610
  819963 |             1570
  936207 |             1556
 1774351 |             1552
  928901 |             1496
  928779 |             1489
 1680262 |             1485
 1735734 |             1463
  753651 |             1459
 1737599 |             1381
  740953 |             1368
 1735934 |             1350
 1720352 |             1330
  630717 |             1325
  820110 |             1293
 1754759 |             1273
(20 rows)
*/
	
-- q6. For each decade, find the most prolific author in that decade. 

CREATE TABLE tempYearAuthor (
		Year INT,
		AuthorID INT,
		NumPublications INT
		);
INSERT INTO tempYearAuthor (
		SELECT CAST(p.Year AS INT), ad.AuthorID, COUNT(PubKey)
			FROM Publication AS p INNER JOIN Authored AS ad ON p.PubID = ad.PubID
			WHERE p.Year IS NOT NULL
			GROUP BY p.Year, ad.AuthorID);


WITH tmp AS (SELECT t1.Year AS StartYear, t1.AuthorID, SUM(t2.NumPublications) AS TotalNum
				FROM tempYearAuthor AS t1 
					INNER JOIN tempYearAuthor AS t2 ON t1.AuthorID = t2.AuthorID
				WHERE t1.Year <= t2.Year AND 
					  t2.Year < t1.Year + 10 AND
					  t1.Year <= 2008
				GROUP BY t1.Year, t1.AuthorID)
SELECT StartYear, AuthorID
	FROM tmp
	WHERE (StartYear, TotalNum) IN (SELECT StartYear, MAX(TotalNum)
										   	   FROM tmp
										   	   GROUP BY StartYear);

DROP TABLE tempYearAuthor;

/*
Result:

 startyear | authorid 
-----------+----------
      1936 |  1672302
      1937 |  1672302
      1938 |  1672302
      1939 |   669963
      1940 |  1672302
      1941 |   492212
      1941 |  1672302
      1942 |   492212
      1943 |  1177870
      1943 |  1307496
      1944 |   492212
      1945 |  1672302
      1946 |  1672302
      1947 |  1672302
      1948 |   583279
      1949 |   778427
      1950 |   583279
      1951 |   778427
      1952 |   583279
      1953 |   583279
      1954 |   352211
      1954 |  1346146
      1955 |   195191
      1956 |  1177931
      1957 |  1436838
      1958 |  1455741
      1959 |  1455741
      1960 |   600978
      1961 |   600978
      1961 |  1455741
      1962 |  1455741
      1963 |  1455741
      1964 |  1455741
      1965 |   723970
      1966 |   723970
      1967 |   723970
      1968 |   723970
      1969 |   723970
      1970 |   156022
      1971 |   547497
      1972 |   547497
      1973 |   156022
      1974 |   156022
      1975 |   156022
      1976 |   156022
      1977 |   156022
      1978 |   156022
      1979 |   156022
      1980 |   156022
      1981 |   156022
      1982 |   156022
      1983 |   156022
      1984 |  1089254
      1985 |  1089254
      1986 |  1089254
      1987 |  1089254
      1988 |   845772
      1989 |   845772
      1990 |   845772
      1991 |  1620924
      1992 |  1620924
      1993 |  1594089
      1994 |  1594089
      1995 |  1594089
      1996 |   411969
      1997 |  1594089
      1998 |  1685741
      1999 |  1685741
      2000 |  1685741
      2001 |  1685741
      2002 |   567942
      2003 |   567942
      2004 |   567942
      2005 |   567942
      2006 |   567942
      2007 |   567942
      2008 |   567942
(77 rows)
*/

-- q7. Find the institutions that have published most papers in STOC; Return the top 20 institutions.

CREATE VIEW Insti AS (
		SELECT AuthorID, SPLIT_PART(Homepage, '/', 3) AS Institution
			FROM Author
			WHERE Homepage IS NOT NULL);
			
SELECT i.Institution, COUNT(DISTINCT ad.PubID) AS NumPublications
	FROM STOC AS s
		INNER JOIN Publication AS p ON s.PubKey = p.PubKey
		INNER JOIN Authored AS ad ON p.PubID = ad.PubID
		INNER JOIN Insti AS i ON ad.AuthorID = i.AuthorID
	GROUP BY i.Institution
	ORDER BY NumPublications DESC
	LIMIT 20;

/*
Result:

        institution        | numpublications 
---------------------------+-----------------
 en.wikipedia.org          |             314
 zbmath.org                |             210
 www.genealogy.ams.org     |             194
 dl.acm.org                |             148
 scholar.google.com        |             128
 www.wisdom.weizmann.ac.il |             111
 viaf.org                  |             104
 id.loc.gov                |              93
 research.microsoft.com    |              80
 theory.stanford.edu       |              72
 www.math.tau.ac.il        |              65
 www.cs.tau.ac.il          |              58
 www.cs.cornell.edu        |              55
 www.cs.huji.ac.il         |              54
 www.cs.princeton.edu      |              54
 www.cs.ucla.edu           |              53
 www.cs.cmu.edu            |              48
 www.eecs.harvard.edu      |              44
 www.cs.berkeley.edu       |              42
 d-nb.info                 |              41
(20 rows)
*/


SELECT i.Institution, COUNT(DISTINCT ad.PubID) AS NumPublications
	FROM PODS AS po
		INNER JOIN Publication AS p ON po.PubKey = p.PubKey
		INNER JOIN Authored AS ad ON p.PubID = ad.PubID
		INNER JOIN Insti AS i ON ad.AuthorID = i.AuthorID
	GROUP BY i.Institution
	ORDER BY NumPublications DESC
	LIMIT 20;

/*
Result:

      institution      | numpublications 
-----------------------+-----------------
 en.wikipedia.org      |             227
 dl.acm.org            |             180
 scholar.google.com    |             114
 www.genealogy.ams.org |              41
 zbmath.org            |              36
 www-cse.ucsd.edu      |              34
 viaf.org              |              31
 www.cs.rice.edu       |              29
 id.loc.gov            |              27
 www.cs.ucsb.edu       |              25
 www.cs.rutgers.edu    |              24
 www.cs.duke.edu       |              21
 www.almaden.ibm.com   |              20
 www-rocq.inria.fr     |              20
 www.cs.indiana.edu    |              20
 alpha.luc.ac.be       |              19
 www.cis.upenn.edu     |              19
 www.soe.ucsc.edu      |              19
 www.dis.uniroma1.it   |              19
 www.research.att.com  |              18
(20 rows)
*/


SELECT i.Institution, COUNT(DISTINCT ad.PubID) AS NumPublications
	FROM SIGMOD AS s
		INNER JOIN Publication AS p ON s.PubKey = p.PubKey
		INNER JOIN Authored AS ad ON p.PubID = ad.PubID
		INNER JOIN Insti AS i ON ad.AuthorID = i.AuthorID
	GROUP BY i.Institution
	ORDER BY NumPublications DESC
	LIMIT 20;

/*
Result:

      institution       | numpublications 
------------------------+-----------------
 dl.acm.org             |             800
 scholar.google.com     |             554
 en.wikipedia.org       |             402
 research.microsoft.com |             181
 viaf.org               |             135
 www.cs.wisc.edu        |             119
 www.comp.nus.edu.sg    |              96
 www.genealogy.ams.org  |              85
 www.research.att.com   |              83
 dais.cs.uiuc.edu       |              82
 www.almaden.ibm.com    |              80
 pages.cs.wisc.edu      |              80
 amturing.acm.org       |              74
 www.cs.columbia.edu    |              57
 id.loc.gov             |              53
 www.cs.ust.hk          |              52
 www.cs.cmu.edu         |              49
 www.cs.umd.edu         |              48
 d-nb.info              |              47
 www.cs.ucsb.edu        |              46
(20 rows)
*/
	
	
DROP VIEW Insti;
DROP MATERIALIZED VIEW STOC;
DROP MATERIALIZED VIEW PODS;
DROP MATERIALIZED VIEW SIGMOD;


-- 5. Using a DBMS from Python or Java and Data Visualization.

SELECT NumCollaborators, COUNT(ID1) AS NumAuthors
	FROM (SELECT ID1, COUNT(DISTINCT ID2) AS NumCollaborators
			  FROM CoAuthor
			  GROUP BY ID1) AS NumColla
	GROUP BY NumCollaborators
	ORDER BY NumCollaborators;
	
SELECT NumPublications, COUNT(AuthorID) AS NumAuthors
	FROM (SELECT AuthorID, COUNT(PubID) AS NumPublications
			  FROM Authored
			  GROUP BY AuthorID) AS AuthorPub
	GROUP BY NumPublications
	ORDER BY NumPublications;
	
-- We can see that both graphs have an approximate distribution of a power law.

-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era) AS
SELECT  MAX(era)
FROM pitching 
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear) AS
SELECT  namefirst
       ,namelast
       ,birthyear
FROM people
WHERE people.weight > 300 
; 

-- Question 1ii
-- 本题可以使用`LIKE`, 即 `WHERE namefirst like '% %'` --case-insensitive
-- 使用`LIKE`需要注意如果搜索的字符包含`%`,就需要特别处理
-- 也可使用正则表达式`WHERE namefirst ~ '.*\s.*`; 此处存疑，SQLite不支持正则表达式匹配
-- 更通用的方式是使用`instr`, 不需要考虑搜索的字符串中包含`%`等特殊字符的情况
-- 注意：`instr`在SQLite 3.7.15及其之后的版本才被支持
CREATE VIEW q1ii(namefirst, namelast, birthyear) AS
SELECT  namefirst
       ,namelast
       ,birthyear
FROM people
WHERE instr(namefirst, ' ')
ORDER BY namefirst ASC, namelast ASC ; 

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count) AS
SELECT  birthyear
       ,AVG(height)
       ,COUNT(*)
FROM people
GROUP BY  birthyear
ORDER BY birthyear 
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count) AS
SELECT  *
FROM q1iii
WHERE avgheight > 70
ORDER BY birthyear 
; 

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid) AS
SELECT  p.namefirst
       ,p.namelast
       ,p.playerid
       ,h.yearid
FROM people AS p, halloffame AS h
WHERE (p.playerid = h.playerid) 
AND h.inducted = 'Y'
ORDER BY h.yearid DESC, p.playerid 
; 

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid) AS
SELECT  q2i.namefirst 
       ,q2i.namelast 
       ,q2i.playerid 
       ,CollegePlaying.schoolid 
       ,q2i.yearid
FROM q2i, CollegePlaying
WHERE (CollegePlaying.playerid = q2i.playerid) 
AND CollegePlaying.schoolid IN ( SELECT Schools.schoolid FROM Schools WHERE Schools.schoolState = 'CA')
ORDER BY q2i.yearid DESC, q2i.playerid 
;                                                              

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid) AS
SELECT  q2i.playerid
       ,q2i.namefirst
       ,q2i.namelast
       ,CollegePlaying.schoolid
FROM q2i
LEFT OUTER JOIN CollegePlaying
ON q2i.playerid = CollegePlaying.playerid
ORDER BY q2i.playerid DESC, schoolid 
; 

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT 1, 1, 1, 1, 1 -- replace this line
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT 1, 1, 1, 1 -- replace this line
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT 1, 1, 1 -- replace this line
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT 1, 1, 1, 1 -- replace this line
;


-- Helper table for 4ii
DROP TABLE IF EXISTS binids;
CREATE TABLE binids(binid);
INSERT INTO binids VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  SELECT 1, 1, 1, 1 -- replace this line
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  SELECT 1, 1, 1, 1 -- replace this line
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT 1, 1, 1, 1, 1 -- replace this line
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT 1, 1 -- replace this line
;


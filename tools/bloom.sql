delimiter //

DROP FUNCTION IF EXISTS hex2dec //

CREATE FUNCTION hex2dec(hexnum TEXT) RETURNS BIGINT
COMMENT 'HELPER FUNCTION converts hex-string to decimal number'
DETERMINISTIC
BEGIN
	DECLARE i                 INT DEFAULT 1;
	DECLARE digits            INT;
	DECLARE result            BIGINT DEFAULT 0;
	DECLARE current_digit     CHAR(1);
	DECLARE current_digit_dec INT;
	SET hexnum = UPPER(hexnum);
	SET digits = LENGTH(hexnum);
	WHILE i <= digits DO
		SET current_digit = SUBSTR(hexnum, i, 1);
		IF current_digit IN ('A','B','C','D','E','F') THEN
			SET current_digit_dec = ASCII(current_digit) - ASCII('A') + 10;
		ELSE
			SET current_digit_dec = ASCII(current_digit) - ASCII('0');
		END IF;
		SET result = (result * 16) + current_digit_dec;
		SET i = i + 1;
	END WHILE;
	RETURN result;
END;
//

DROP FUNCTION IF EXISTS powerfactor //

CREATE FUNCTION powerfactor(rowcount BIGINT) RETURNS INT
COMMENT 'HELPER FUNCTION computes the power factor out of the row count (min is 2 i.e. 256 bits; max is 16 i.e. 2^64 bits; max is 9 i.e. 2^30 columns)'
DETERMINISTIC
BEGIN
	DECLARE powfac INT;
	IF rowcount < 1 THEN
		SET rowcount = 1;
	END IF;
	SET powfac = ceiling(log(rowcount)/log(16));
	IF powfac < 2 THEN
		SET powfac = 2;
	END IF;
	IF powfac > 9 THEN
		SET powfac = 9;
	END IF;
	RETURN powfac;
END;
//

DROP FUNCTION IF EXISTS eucrem //

CREATE FUNCTION eucrem(n BIGINT, d INT) RETURNS INT
COMMENT 'HELPER FUNCTION computes euclidean remainder'
DETERMINISTIC
BEGIN
	RETURN n - d * eucqot(n, d);
END;
//

DROP FUNCTION IF EXISTS eucqot //

CREATE FUNCTION eucqot(n BIGINT, d INT) RETURNS INT
COMMENT 'HELPER FUNCTION computes euclidean quotient'
DETERMINISTIC
BEGIN
	RETURN floor(n / d);
END;
//

DROP FUNCTION IF EXISTS bloom //

CREATE FUNCTION bloom(t TEXT, powfac INT) RETURNS BIGINT
COMMENT 'HELPER FUNCTION computes bloom value of text; powfac (x) is the power factor of the filter, it is log base 16 of the bit-array size, so if x = 3 then the bit-array size is 16^3; x should be >= 2'
DETERMINISTIC
BEGIN
	RETURN hex2dec(RIGHT(SHA1(t), powfac));
END;
//

DROP FUNCTION IF EXISTS bloomquery //

CREATE FUNCTION bloomquery(colname TEXT, tblquery TEXT, powfac INT) RETURNS TEXT
COMMENT 'HELPER FUNCTION returns simple bloom query'
DETERMINISTIC
BEGIN
	RETURN CONCAT('select distinct bloom(`', colname, '`, ', powfac, ') as bloom_col from (', tblquery, ') originaltable');
END;
//

DROP FUNCTION IF EXISTS bvbloomquery //

CREATE FUNCTION bvbloomquery(colname TEXT, tblquery TEXT, powfac INT) RETURNS TEXT
COMMENT 'HELPER FUNCTION returns bit-vector bloom query'
DETERMINISTIC
BEGIN
	DECLARE qry TEXT;
	SET qry = CONCAT('select floor(bloom_col / 64) as __id, bit_or(1 << (bloom_col - 64 * floor(bloom_col / 64))) as __val');
	SET qry = CONCAT(qry, ' from (', bloomquery(colname, tblquery, powfac), ') simplebloomtable group by 1');
	RETURN qry;
END;
//

DROP FUNCTION IF EXISTS bvbloomcreatetemptbl //

CREATE FUNCTION bvbloomcreatetemptbl(temptblname TEXT) RETURNS TEXT
COMMENT 'HELPER FUNCTION returns query to create temp table to host bit-vector bloom filter'
DETERMINISTIC
BEGIN
	DECLARE qry TEXT;
	SET qry = CONCAT('CREATE TABLE ', temptblname, ' (__id INT UNSIGNED NOT NULL PRIMARY KEY, __val BIGINT UNSIGNED NOT NULL)');
	RETURN qry;
END;
//

DROP FUNCTION IF EXISTS bvbloomcondcheck //

CREATE FUNCTION bvbloomcondcheck(colname TEXT, tblname TEXT, powfac INT) RETURNS TEXT
COMMENT 'HELPER FUNCTION returns the condition check that is used to filter tuples according to bloom'
DETERMINISTIC
BEGIN
	DECLARE qry TEXT;
	SET qry = CONCAT('(((1 << eucrem(bloom(', colname, ', ', powfac, '), 64)) & (select __val from ', tblname, ' where eucqot(bloom(', colname, ', ', powfac, '), 64) = __id)) <> 0)');
	RETURN qry;
END;
//

DROP PROCEDURE IF EXISTS createemptybloomfilter //

CREATE PROCEDURE createemptybloomfilter(IN temptblname TEXT)
COMMENT 'INTERFACE TO CREATE EMPTY TABLE FOR BLOOM FILTER'
BEGIN
	
	SET @qry = CONCAT('DROP TABLE IF EXISTS ', temptblname);
	PREPARE stmt1 FROM @qry;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
	SET @qry = bvbloomcreatetemptbl(temptblname);
	PREPARE stmt1 FROM @qry;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
END;
//

DROP PROCEDURE IF EXISTS computebloomfilter //

CREATE PROCEDURE computebloomfilter(IN rowcount BIGINT, IN colname TEXT, IN tblquery TEXT)
COMMENT 'INTERFACE TO COMPUTE BLOOM FILTER'
READS SQL DATA
BEGIN
	
	SET @qry = bvbloomquery(colname, tblquery, powerfactor(rowcount));
	PREPARE stmt1 FROM @qry;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
END;
//

DROP PROCEDURE IF EXISTS filterbybloom //

CREATE PROCEDURE filterbybloom(IN rowcount BIGINT, IN colname TEXT, IN tblquery TEXT, IN temptblname TEXT)
COMMENT 'INTERFACE TO FILTER ACCORDING TO BLOOM'
READS SQL DATA
BEGIN
	
	SET @qry = REPLACE(tblquery, '?', bvbloomcondcheck(colname, temptblname, powerfactor(rowcount)));
	PREPARE stmt1 FROM @qry;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
END;
//

delimiter ;
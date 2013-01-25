--
-- CRAP FUNCTIONS
--

CREATE OR REPLACE FUNCTION digest(TEXT, TEXT) RETURNS BYTEA AS
	'$libdir/pgcrypto', 'pg_digest'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION mysha1(TEXT) RETURNS TEXT AS $$
	SELECT encode(digest($1, 'sha1'), 'hex')
$$ LANGUAGE sql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION myright(str TEXT, len INT) RETURNS TEXT AS $$
DECLARE
	x INT;
BEGIN
	IF len < 1 THEN
		RETURN '';
	END IF;
	x := char_length(str);
	IF len > x THEN
		len := x;
	END IF;
	x := x - len + 1;
	RETURN substring(str from x for len);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

--
-- HELPER FUNCTIONS
--

CREATE OR REPLACE FUNCTION hex2dec(hexnum TEXT) RETURNS BIGINT AS $$
DECLARE
	i                 INT DEFAULT 1;
	digits            INT;
	result            BIGINT DEFAULT 0;
	current_digit     CHAR(1);
	current_digit_dec INT;
BEGIN
	hexnum := UPPER(hexnum);
	digits := LENGTH(hexnum);
	WHILE i <= digits LOOP
		current_digit := SUBSTR(hexnum, i, 1);
		IF current_digit IN ('A','B','C','D','E','F') THEN
			current_digit_dec := ASCII(current_digit) - ASCII('A') + 10;
		ELSE
			current_digit_dec := ASCII(current_digit) - ASCII('0');
		END IF;
		result := (result * 16) + current_digit_dec;
		i := i + 1;
	END LOOP;
	RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
-- ALTER FUNCTION public.hex2dec(hexnum TEXT) OWNER TO postgres;
-- COMMENT ON FUNCTION hex2dec(hexnum TEXT) IS 'HELPER FUNCTION converts hex-string to decimal number';

CREATE OR REPLACE FUNCTION powerfactor(rowcount BIGINT) RETURNS INT AS $$
DECLARE
	powfac INT;
BEGIN
	IF rowcount < 1 THEN
		rowcount := 1;
	END IF;
	powfac := ceiling(log(rowcount)/log(16));
	IF powfac < 2 THEN
		powfac := 2;
	END IF;
	IF powfac > 9 THEN
		powfac := 9;
	END IF;
	RETURN powfac;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION eucqot(n BIGINT, d INT) RETURNS INT AS $$
BEGIN
	RETURN floor(n / d);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION eucrem(n BIGINT, d INT) RETURNS INT AS $$
BEGIN
	RETURN n - d * eucqot(n, d);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION bloom(t TEXT, powfac INT) RETURNS BIGINT AS $$
BEGIN
	RETURN hex2dec(myright(mysha1(t), powfac));
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION bloomquery(colname TEXT, tblquery TEXT, powfac INT) RETURNS TEXT AS $$
BEGIN
	RETURN ('select distinct bloom(CAST(' || quote_ident(colname) || ' AS TEXT), ' || powfac || ') as bloom_col from (' || tblquery || ') originaltable');
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION bvbloomquery(colname TEXT, tblquery TEXT, powfac INT) RETURNS TEXT AS $$
DECLARE
	qry TEXT;
BEGIN
	qry := 'select CAST((bloom_col >> 6) AS INT) as __id, bit_or(CAST(1 AS BIGINT) << CAST((bloom_col & 63) AS INT)) as __val';
	qry := (qry || ' from (' || bloomquery(colname, tblquery, powfac) || ') simplebloomtable group by 1');
	RETURN qry;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION bvbloomcreatetemptbl(temptblname TEXT) RETURNS TEXT AS $$
DECLARE
	qry TEXT;
BEGIN
	qry := ('CREATE TABLE ' || temptblname || ' (__id INT NOT NULL PRIMARY KEY, __val BIGINT NOT NULL)');
	RETURN qry;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION bvbloomcondcheck(colname TEXT, tblname TEXT, powfac INT) RETURNS TEXT AS $$
DECLARE
	qry TEXT;
BEGIN
	qry := ('(((CAST(1 AS BIGINT) << CAST((bloom(CAST(' || quote_ident(colname) || ' AS TEXT), ' || powfac || ') & 63) AS INT)) & (select __val from ' || tblname || ' where (bloom(CAST(' || quote_ident(colname) || ' AS TEXT), ' || powfac || ') >> 6) = __id)) <> 0)');
	RETURN qry;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

--
-- INTERFACE
--

CREATE OR REPLACE FUNCTION createemptybloomfilter(IN temptblname TEXT) RETURNS VOID AS $$
BEGIN
	EXECUTE ('DROP TABLE IF EXISTS ' || temptblname);
	EXECUTE bvbloomcreatetemptbl(temptblname);
END;
$$ LANGUAGE plpgsql;
-- USAGE select createemptybloomfilter('test_bloom2');

CREATE OR REPLACE FUNCTION computebloomfilter(IN rowcount BIGINT, IN colname TEXT, IN tblquery TEXT) RETURNS TABLE(__id INT, __val BIGINT) AS $$
BEGIN
	RETURN QUERY
		EXECUTE bvbloomquery(colname, tblquery, powerfactor(rowcount));
END;
$$ LANGUAGE plpgsql;
-- USAGE select * into test_bloom1 from computebloomfilter(7, 'col1', 'select \'Arthur\' as col1 union select \'Ford\' as col1');

CREATE OR REPLACE FUNCTION filterbybloom(IN rowcount BIGINT, IN colname TEXT, IN tblquery TEXT, IN temptblname TEXT) RETURNS SETOF RECORD AS $$
BEGIN
	RETURN QUERY
		EXECUTE REPLACE(tblquery, '?', bvbloomcondcheck(colname, temptblname, powerfactor(rowcount)));
END;
$$ LANGUAGE plpgsql;
-- USAGE select * from filterbybloom(7, 'first_name', 'select * from employee where ?', 'test_bloom1') AS tbl(id int, first varchar(50), last varchar(50));
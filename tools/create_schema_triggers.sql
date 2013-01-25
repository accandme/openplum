CREATE OR REPLACE FUNCTION partsupp_checkDuplicatePK() RETURNS TRIGGER AS
$$
	BEGIN
		IF (SELECT 1 FROM partsupp WHERE ps_partkey = NEW.ps_partkey AND ps_suppkey = NEW.ps_suppkey) THEN RETURN NULL;
		ELSE RETURN new;
		END IF;
	END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER partsupp_checkDuplicatePK
BEFORE INSERT OR UPDATE ON partsupp
FOR EACH ROW EXECUTE PROCEDURE partsupp_checkDuplicatePK();
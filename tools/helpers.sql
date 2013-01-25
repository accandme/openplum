CREATE OR REPLACE FUNCTION createtableifnotexists(IN tblname TEXT, IN tblschema TEXT) RETURNS VOID AS $$
BEGIN
	IF NOT EXISTS ( SELECT * FROM pg_catalog.pg_tables WHERE tablename = tblname) THEN
		EXECUTE ( 'CREATE TABLE ' || tblname || ' (' || tblschema || ')' );
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION executeinto(IN querytxt TEXT, IN intotbl TEXT) RETURNS VOID AS $$
BEGIN
	IF NOT EXISTS ( SELECT * FROM pg_catalog.pg_tables WHERE tablename = intotbl) THEN
		EXECUTE ( 'CREATE TABLE ' || intotbl || ' AS (' || querytxt || ')' );
	ELSE
		EXECUTE ( 'INSERT INTO ' || intotbl || ' (' || querytxt || ')' );
	END IF;
END;
$$ LANGUAGE plpgsql;
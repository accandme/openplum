/**
 * _intermediate and _final variants of the five standard aggregate functions: AVG, SUM, COUNT, MIN, MAX.
 * Many are identical to their original variant, but still needed to be redefined to satisfy the naming convention.
 * Original PostgreSQL definitions of the five aggregates were taken from: SELECT * FROM pg_aggregate;
 */

/**
 * AVG
 * intermediate is same as standard AVG without finalization, final uses a custom function that merges intermediate results and then does standard finalization.
 */

-- AVG(int2)

DROP AGGREGATE IF EXISTS AVG_intermediate(int2);
CREATE AGGREGATE AVG_intermediate(int2)
(
    sfunc = int2_avg_accum,
    stype = int8[],
    initcond = '{0, 0}'
);

CREATE OR REPLACE FUNCTION AVG_merge(int8[], int8[]) RETURNS int8[] AS
    $$
    SELECT ARRAY(
        SELECT $1[i] + $2[i]
        FROM generate_series(array_lower($1, 1), array_upper($1, 1)) i
        )
    $$
    LANGUAGE SQL;
DROP AGGREGATE IF EXISTS AVG_final(int8[]);
CREATE AGGREGATE AVG_final(int8[])
(
    sfunc = AVG_merge,
    stype = int8[],
    finalfunc = int8_avg,
    initcond = '{0, 0}'
);

-- AVG(int4) --> intermediate only, final same as for int2

DROP AGGREGATE IF EXISTS AVG_intermediate(int4);
CREATE AGGREGATE AVG_intermediate(int4)
(
    sfunc = int4_avg_accum,
    stype = int8[],
    initcond = '{0, 0}'
);

-- AVG(int8)

DROP AGGREGATE IF EXISTS AVG_intermediate(int8);
CREATE AGGREGATE AVG_intermediate(int8)
(
    sfunc = int8_avg_accum,
    stype = numeric[],
    initcond = '{0, 0}'
);

CREATE OR REPLACE FUNCTION AVG_merge(numeric[], numeric[]) RETURNS numeric[] AS
    $$
    SELECT ARRAY(
        SELECT $1[i] + $2[i]
        FROM generate_series(array_lower($1, 1), array_upper($1, 1)) i
        )
    $$
    LANGUAGE SQL;
DROP AGGREGATE IF EXISTS AVG_final(numeric[]);
CREATE AGGREGATE AVG_final(numeric[])
(
    sfunc = AVG_merge,
    stype = numeric[],
    finalfunc = numeric_avg,
    initcond = '{0, 0}'
);

-- AVG(numeric) --> intermediate only, final same as for int8

DROP AGGREGATE IF EXISTS AVG_intermediate(numeric);
CREATE AGGREGATE AVG_intermediate(numeric)
(
    sfunc = numeric_avg_accum,
    stype = numeric[],
    initcond = '{0, 0}'
);

-- AVG(float4)

DROP AGGREGATE IF EXISTS AVG_intermediate(float4);
CREATE AGGREGATE AVG_intermediate(float4)
(
    sfunc = float4_accum,
    stype = float8[],
    initcond = '{0, 0, 0}'
);

CREATE OR REPLACE FUNCTION AVG_merge(float8[], float8[]) RETURNS float8[] AS
    $$
    SELECT ARRAY(
        SELECT $1[i] + $2[i]
        FROM generate_series(array_lower($1, 1), array_upper($1, 1)) i
        )
    $$
    LANGUAGE SQL;
DROP AGGREGATE IF EXISTS AVG_final(float8[]);
CREATE AGGREGATE AVG_final(float8[])
(
    sfunc = AVG_merge,
    stype = float8[],
    finalfunc = float8_avg,
    initcond = '{0, 0, 0}'
);

-- AVG(float8) --> intermediate only, final same as for float4

DROP AGGREGATE IF EXISTS AVG_intermediate(float8);
CREATE AGGREGATE AVG_intermediate(float8)
(
    sfunc = float8_accum,
    stype = float8[],
    initcond = '{0, 0, 0}'
);

-- AVG(interval)

DROP AGGREGATE IF EXISTS AVG_intermediate(interval);
CREATE AGGREGATE AVG_intermediate(interval)
(
    sfunc = interval_accum,
    stype = interval[],
    initcond = '{0 second, 0 second}'
);

CREATE OR REPLACE FUNCTION AVG_merge(interval[], interval[]) RETURNS interval[] AS
    $$
    SELECT ARRAY(
        SELECT $1[i] + $2[i]
        FROM generate_series(array_lower($1, 1), array_upper($1, 1)) i
        )
    $$
    LANGUAGE SQL;
DROP AGGREGATE IF EXISTS AVG_final(interval[]);
CREATE AGGREGATE AVG_final(interval[])
(
    sfunc = AVG_merge,
    stype = interval[],
    finalfunc = interval_avg,
    initcond = '{ 0 second, 0 second }'
);

/**
 * SUM
 * Both intermediate and final are same as standard SUM.
 */

-- SUM(int2)

DROP AGGREGATE IF EXISTS SUM_intermediate(int2);
CREATE AGGREGATE SUM_intermediate(int2)
(
    sfunc = int2_sum,
    stype = int8
);

DROP AGGREGATE IF EXISTS SUM_final(int8);
CREATE AGGREGATE SUM_final(int8)
(
    sfunc = int8_sum,
    stype = numeric
);

-- SUM(int4) --> intermediate only, sum same as for int2

DROP AGGREGATE IF EXISTS SUM_intermediate(int4);
CREATE AGGREGATE SUM_intermediate(int4)
(
    sfunc = int4_sum,
    stype = int8
);

-- SUM(int8)

DROP AGGREGATE IF EXISTS SUM_intermediate(int8);
CREATE AGGREGATE SUM_intermediate(int8)
(
    sfunc = int8_sum,
    stype = numeric
);

DROP AGGREGATE IF EXISTS SUM_final(numeric);
CREATE AGGREGATE SUM_final(numeric)
(
    sfunc = numeric_add,
    stype = numeric
);

-- SUM(numeric) --> intermediate only, final same as for int8

DROP AGGREGATE IF EXISTS SUM_intermediate(numeric);
CREATE AGGREGATE SUM_intermediate(numeric)
(
    sfunc = numeric_add,
    stype = numeric
);

-- SUM(float4)

DROP AGGREGATE IF EXISTS SUM_intermediate(float4);
CREATE AGGREGATE SUM_intermediate(float4)
(
    sfunc = float4pl,
    stype = float4
);

DROP AGGREGATE IF EXISTS SUM_final(float4);
CREATE AGGREGATE SUM_final(float4)
(
    sfunc = float4pl,
    stype = float4
);

-- SUM(float8)

DROP AGGREGATE IF EXISTS SUM_intermediate(float8);
CREATE AGGREGATE SUM_intermediate(float8)
(
    sfunc = float8pl,
    stype = float8
);

DROP AGGREGATE IF EXISTS SUM_final(float8);
CREATE AGGREGATE SUM_final(float8)
(
    sfunc = float8pl,
    stype = float8
);

-- SUM(interval)

DROP AGGREGATE IF EXISTS SUM_intermediate(interval);
CREATE AGGREGATE SUM_intermediate(interval)
(
    sfunc = interval_pl,
    stype = interval
);

DROP AGGREGATE IF EXISTS SUM_final(interval);
CREATE AGGREGATE SUM_final(interval)
(
    sfunc = interval_pl,
    stype = interval
);

-- SUM(money)

DROP AGGREGATE IF EXISTS SUM_intermediate(money);
CREATE AGGREGATE SUM_intermediate(money)
(
    sfunc = cash_pl,
    stype = money
);

DROP AGGREGATE IF EXISTS SUM_final(money);
CREATE AGGREGATE SUM_final(money)
(
    sfunc = cash_pl,
    stype = money
);

/**
 * COUNT
 * intermediate is same as regular COUNT, final is same as regular SUM.
 */
 
-- COUNT(*) (no arguments)
 
DROP AGGREGATE IF EXISTS COUNT_intermediate(*);
CREATE AGGREGATE COUNT_intermediate(*)
(
    sfunc = int8inc,
    stype = int8,
	initcond = 0
);

DROP AGGREGATE IF EXISTS COUNT_final(int8);
CREATE AGGREGATE COUNT_final(int8)
(
    sfunc = int8_sum,
    stype = numeric,
    initcond = 0
);

-- COUNT("any") --> intermediate only, final same as for *

DROP AGGREGATE IF EXISTS COUNT_intermediate("any");
CREATE AGGREGATE COUNT_intermediate("any")
(
    sfunc = int8inc_any,
    stype = int8,
    initcond = 0
);

/**
 * MIN
 * Both intermediate and final are same as regular MIN.
 */

-- MIN(int2)

DROP AGGREGATE IF EXISTS MIN_intermediate(int2);
CREATE AGGREGATE MIN_intermediate(int2)
(
    sfunc = int2smaller,
    stype = int2,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(int2);
CREATE AGGREGATE MIN_final(int2)
(
    sfunc = int2smaller,
    stype = int2,
    sortop = >
);

-- MIN(int4)

DROP AGGREGATE IF EXISTS MIN_intermediate(int4);
CREATE AGGREGATE MIN_intermediate(int4)
(
    sfunc = int4smaller,
    stype = int4,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(int4);
CREATE AGGREGATE MIN_final(int4)
(
    sfunc = int4smaller,
    stype = int4,
    sortop = >
);

-- MIN(int8)

DROP AGGREGATE IF EXISTS MIN_intermediate(int8);
CREATE AGGREGATE MIN_intermediate(int8)
(
    sfunc = int8smaller,
    stype = int8,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(int8);
CREATE AGGREGATE MIN_final(int8)
(
    sfunc = int8smaller,
    stype = int8,
    sortop = >
);

-- MIN(numeric)

DROP AGGREGATE IF EXISTS MIN_intermediate(numeric);
CREATE AGGREGATE MIN_intermediate(numeric)
(
    sfunc = numeric_smaller,
    stype = numeric,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(numeric);
CREATE AGGREGATE MIN_final(numeric)
(
    sfunc = numeric_smaller,
    stype = numeric,
    sortop = >
);

-- MIN(float4)

DROP AGGREGATE IF EXISTS MIN_intermediate(float4);
CREATE AGGREGATE MIN_intermediate(float4)
(
    sfunc = float4smaller,
    stype = float4,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(float4);
CREATE AGGREGATE MIN_final(float4)
(
    sfunc = float4smaller,
    stype = float4,
    sortop = >
);

-- MIN(float8)

DROP AGGREGATE IF EXISTS MIN_intermediate(float8);
CREATE AGGREGATE MIN_intermediate(float8)
(
    sfunc = float8smaller,
    stype = float8,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(float8);
CREATE AGGREGATE MIN_final(float8)
(
    sfunc = float8smaller,
    stype = float8,
    sortop = >
);

-- MIN(interval)

DROP AGGREGATE IF EXISTS MIN_intermediate(interval);
CREATE AGGREGATE MIN_intermediate(interval)
(
    sfunc = interval_smaller,
    stype = interval,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(interval);
CREATE AGGREGATE MIN_final(interval)
(
    sfunc = interval_smaller,
    stype = interval,
    sortop = >
);

-- MIN(date)

DROP AGGREGATE IF EXISTS MIN_intermediate(date);
CREATE AGGREGATE MIN_intermediate(date)
(
    sfunc = date_smaller,
    stype = date,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(date);
CREATE AGGREGATE MIN_final(date)
(
    sfunc = date_smaller,
    stype = date,
    sortop = >
);

-- MIN(time)

DROP AGGREGATE IF EXISTS MIN_intermediate(time);
CREATE AGGREGATE MIN_intermediate(time)
(
    sfunc = time_smaller,
    stype = time,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(time);
CREATE AGGREGATE MIN_final(time)
(
    sfunc = time_smaller,
    stype = time,
    sortop = >
);

-- MIN(timetz)

DROP AGGREGATE IF EXISTS MIN_intermediate(timetz);
CREATE AGGREGATE MIN_intermediate(timetz)
(
    sfunc = timetz_smaller,
    stype = timetz,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(timetz);
CREATE AGGREGATE MIN_final(timetz)
(
    sfunc = timetz_smaller,
    stype = timetz,
    sortop = >
);

-- MIN(timestamp)

DROP AGGREGATE IF EXISTS MIN_intermediate(timestamp);
CREATE AGGREGATE MIN_intermediate(timestamp)
(
    sfunc = timestamp_smaller,
    stype = timestamp,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(timestamp);
CREATE AGGREGATE MIN_final(timestamp)
(
    sfunc = timestamp_smaller,
    stype = timestamp,
    sortop = >
);

-- MIN(timestamptz)

DROP AGGREGATE IF EXISTS MIN_intermediate(timestamptz);
CREATE AGGREGATE MIN_intermediate(timestamptz)
(
    sfunc = timestamptz_smaller,
    stype = timestamptz,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(timestamptz);
CREATE AGGREGATE MIN_final(timestamptz)
(
    sfunc = timestamptz_smaller,
    stype = timestamptz,
    sortop = >
);

-- MIN(money)

DROP AGGREGATE IF EXISTS MIN_intermediate(money);
CREATE AGGREGATE MIN_intermediate(money)
(
    sfunc = cashsmaller,
    stype = money,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(money);
CREATE AGGREGATE MIN_final(money)
(
    sfunc = cashsmaller,
    stype = money,
    sortop = >
);

-- MIN(oid)

DROP AGGREGATE IF EXISTS MIN_intermediate(oid);
CREATE AGGREGATE MIN_intermediate(oid)
(
    sfunc = oidsmaller,
    stype = oid,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(oid);
CREATE AGGREGATE MIN_final(oid)
(
    sfunc = oidsmaller,
    stype = oid,
    sortop = >
);

-- MIN(text)

DROP AGGREGATE IF EXISTS MIN_intermediate(text);
CREATE AGGREGATE MIN_intermediate(text)
(
    sfunc = text_smaller,
    stype = text,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(text);
CREATE AGGREGATE MIN_final(text)
(
    sfunc = text_smaller,
    stype = text,
    sortop = >
);

-- MIN(bpchar)

DROP AGGREGATE IF EXISTS MIN_intermediate(bpchar);
CREATE AGGREGATE MIN_intermediate(bpchar)
(
    sfunc = bpchar_smaller,
    stype = bpchar,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(bpchar);
CREATE AGGREGATE MIN_final(bpchar)
(
    sfunc = bpchar_smaller,
    stype = bpchar,
    sortop = >
);

-- MIN(tid)

DROP AGGREGATE IF EXISTS MIN_intermediate(tid);
CREATE AGGREGATE MIN_intermediate(tid)
(
    sfunc = tidsmaller,
    stype = tid,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(tid);
CREATE AGGREGATE MIN_final(tid)
(
    sfunc = tidsmaller,
    stype = tid,
    sortop = >
);

-- MIN(anyenum)
/* throws error: return type of transition function enum_smaller is not anyenum
DROP AGGREGATE IF EXISTS MIN_intermediate(anyenum);
CREATE AGGREGATE MIN_intermediate(anyenum)
(
    sfunc = enum_smaller,
    stype = anyenum,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(anyenum);
CREATE AGGREGATE MIN_final(anyenum)
(
    sfunc = enum_smaller,
    stype = anyenum,
    sortop = >
);
*/
-- MIN(anyarray)

DROP AGGREGATE IF EXISTS MIN_intermediate(anyarray);
CREATE AGGREGATE MIN_intermediate(anyarray)
(
    sfunc = array_smaller,
    stype = anyarray,
    sortop = >
);

DROP AGGREGATE IF EXISTS MIN_final(anyarray);
CREATE AGGREGATE MIN_final(anyarray)
(
    sfunc = array_smaller,
    stype = anyarray,
    sortop = >
);

/**
 * MAX
 * Both intermediate and final are same as regular MAX.
 */

-- MAX(int2)

DROP AGGREGATE IF EXISTS MAX_intermediate(int2);
CREATE AGGREGATE MAX_intermediate(int2)
(
    sfunc = int2larger,
    stype = int2,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(int2);
CREATE AGGREGATE MAX_final(int2)
(
    sfunc = int2larger,
    stype = int2,
    sortop = <
);

-- MAX(int4)

DROP AGGREGATE IF EXISTS MAX_intermediate(int4);
CREATE AGGREGATE MAX_intermediate(int4)
(
    sfunc = int4larger,
    stype = int4,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(int4);
CREATE AGGREGATE MAX_final(int4)
(
    sfunc = int4larger,
    stype = int4,
    sortop = <
);

-- MAX(int8)

DROP AGGREGATE IF EXISTS MAX_intermediate(int8);
CREATE AGGREGATE MAX_intermediate(int8)
(
    sfunc = int8larger,
    stype = int8,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(int8);
CREATE AGGREGATE MAX_final(int8)
(
    sfunc = int8larger,
    stype = int8,
    sortop = <
);

-- MAX(numeric)

DROP AGGREGATE IF EXISTS MAX_intermediate(numeric);
CREATE AGGREGATE MAX_intermediate(numeric)
(
    sfunc = numeric_larger,
    stype = numeric,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(numeric);
CREATE AGGREGATE MAX_final(numeric)
(
    sfunc = numeric_larger,
    stype = numeric,
    sortop = <
);

-- MAX(float4)

DROP AGGREGATE IF EXISTS MAX_intermediate(float4);
CREATE AGGREGATE MAX_intermediate(float4)
(
    sfunc = float4larger,
    stype = float4,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(float4);
CREATE AGGREGATE MAX_final(float4)
(
    sfunc = float4larger,
    stype = float4,
    sortop = <
);

-- MAX(float8)

DROP AGGREGATE IF EXISTS MAX_intermediate(float8);
CREATE AGGREGATE MAX_intermediate(float8)
(
    sfunc = float8larger,
    stype = float8,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(float8);
CREATE AGGREGATE MAX_final(float8)
(
    sfunc = float8larger,
    stype = float8,
    sortop = <
);

-- MAX(interval)

DROP AGGREGATE IF EXISTS MAX_intermediate(interval);
CREATE AGGREGATE MAX_intermediate(interval)
(
    sfunc = interval_larger,
    stype = interval,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(interval);
CREATE AGGREGATE MAX_final(interval)
(
    sfunc = interval_larger,
    stype = interval,
    sortop = <
);

-- MAX(date)

DROP AGGREGATE IF EXISTS MAX_intermediate(date);
CREATE AGGREGATE MAX_intermediate(date)
(
    sfunc = date_larger,
    stype = date,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(date);
CREATE AGGREGATE MAX_final(date)
(
    sfunc = date_larger,
    stype = date,
    sortop = <
);

-- MAX(time)

DROP AGGREGATE IF EXISTS MAX_intermediate(time);
CREATE AGGREGATE MAX_intermediate(time)
(
    sfunc = time_larger,
    stype = time,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(time);
CREATE AGGREGATE MAX_final(time)
(
    sfunc = time_larger,
    stype = time,
    sortop = <
);

-- MAX(timetz)

DROP AGGREGATE IF EXISTS MAX_intermediate(timetz);
CREATE AGGREGATE MAX_intermediate(timetz)
(
    sfunc = timetz_larger,
    stype = timetz,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(timetz);
CREATE AGGREGATE MAX_final(timetz)
(
    sfunc = timetz_larger,
    stype = timetz,
    sortop = <
);

-- MAX(timestamp)

DROP AGGREGATE IF EXISTS MAX_intermediate(timestamp);
CREATE AGGREGATE MAX_intermediate(timestamp)
(
    sfunc = timestamp_larger,
    stype = timestamp,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(timestamp);
CREATE AGGREGATE MAX_final(timestamp)
(
    sfunc = timestamp_larger,
    stype = timestamp,
    sortop = <
);

-- MAX(timestamptz)

DROP AGGREGATE IF EXISTS MAX_intermediate(timestamptz);
CREATE AGGREGATE MAX_intermediate(timestamptz)
(
    sfunc = timestamptz_larger,
    stype = timestamptz,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(timestamptz);
CREATE AGGREGATE MAX_final(timestamptz)
(
    sfunc = timestamptz_larger,
    stype = timestamptz,
    sortop = <
);

-- MAX(money)

DROP AGGREGATE IF EXISTS MAX_intermediate(money);
CREATE AGGREGATE MAX_intermediate(money)
(
    sfunc = cashlarger,
    stype = money,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(money);
CREATE AGGREGATE MAX_final(money)
(
    sfunc = cashlarger,
    stype = money,
    sortop = <
);

-- MAX(oid)

DROP AGGREGATE IF EXISTS MAX_intermediate(oid);
CREATE AGGREGATE MAX_intermediate(oid)
(
    sfunc = oidlarger,
    stype = oid,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(oid);
CREATE AGGREGATE MAX_final(oid)
(
    sfunc = oidlarger,
    stype = oid,
    sortop = <
);

-- MAX(text)

DROP AGGREGATE IF EXISTS MAX_intermediate(text);
CREATE AGGREGATE MAX_intermediate(text)
(
    sfunc = text_larger,
    stype = text,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(text);
CREATE AGGREGATE MAX_final(text)
(
    sfunc = text_larger,
    stype = text,
    sortop = <
);

-- MAX(bpchar)

DROP AGGREGATE IF EXISTS MAX_intermediate(bpchar);
CREATE AGGREGATE MAX_intermediate(bpchar)
(
    sfunc = bpchar_larger,
    stype = bpchar,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(bpchar);
CREATE AGGREGATE MAX_final(bpchar)
(
    sfunc = bpchar_larger,
    stype = bpchar,
    sortop = <
);

-- MAX(tid)

DROP AGGREGATE IF EXISTS MAX_intermediate(tid);
CREATE AGGREGATE MAX_intermediate(tid)
(
    sfunc = tidlarger,
    stype = tid,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(tid);
CREATE AGGREGATE MAX_final(tid)
(
    sfunc = tidlarger,
    stype = tid,
    sortop = <
);

-- MAX(anyenum)
/* throws error: return type of transition function enum_larger is not anyenum
DROP AGGREGATE IF EXISTS MAX_intermediate(anyenum);
CREATE AGGREGATE MAX_intermediate(anyenum)
(
    sfunc = enum_larger,
    stype = anyenum,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(anyenum);
CREATE AGGREGATE MAX_final(anyenum)
(
    sfunc = enum_larger,
    stype = anyenum,
    sortop = <
);
*/
-- MAX(anyarray)

DROP AGGREGATE IF EXISTS MAX_intermediate(anyarray);
CREATE AGGREGATE MAX_intermediate(anyarray)
(
    sfunc = array_larger,
    stype = anyarray,
    sortop = <
);

DROP AGGREGATE IF EXISTS MAX_final(anyarray);
CREATE AGGREGATE MAX_final(anyarray)
(
    sfunc = array_larger,
    stype = anyarray,
    sortop = <
);
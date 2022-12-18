CREATE DATABASE IF NOT EXISTS creator;

CREATE TABLE IF NOT EXISTS creator.clients (
    msisdn UInt64,
    gender char(1),
    age UInt8,
    income Float32,
    counter UInt32
) ENGINE = MergeTree()
ORDER BY (msisdn);


CREATE TABLE IF NOT EXISTS creator.segments (
    id UUID,
    msisdn UInt64
) ENGINE = MergeTree()
PARTITION BY id
ORDER BY (id);


-- CREATE TABLE IF NOT EXISTS creator.counters (
--     msisdn UInt64,
--     counter UInt32
-- ) ENGINE = MergeTree()
-- ORDER BY (msisdn);

--
-- CREATE VIEW IF NOT EXISTS creator.sum_msisdn_counter
-- AS SELECT msisdn, sum(counter) as sum_counter
--    FROM creator.counters
--    GROUP BY msisdn;


SET mutations_sync = 1;

-- CREATE TABLE IF NOT EXISTS creator.clients (
--     msisdn Int64,
--     gender char(1),
--     age smallint,
--     income decimal(10,2),
--     counter integer
-- );




-- CREATE TABLE IF NOT EXISTS graphite.metrics (
--                                                 date Date DEFAULT toDate(0),
--                                                 name String,
--                                                 level UInt16,
--                                                 parent String,
--                                                 updated DateTime DEFAULT now(),
--                                                 status Enum8('SIMPLE' = 0, 'BAN' = 1, 'APPROVED' = 2, 'HIDDEN' = 3, 'AUTO_HIDDEN' = 4)
-- )
--     ENGINE = ReplacingMergeTree(updated)
--         PARTITION BY toYYYYMM(date)
--         ORDER BY (parent, name)
--         SETTINGS index_granularity = 1024;
--
-- CREATE TABLE IF NOT EXISTS graphite.data (
--                                              metric String,
--                                              value Float64,
--                                              timestamp UInt32,
--                                              date Date,  updated UInt32
-- )
--     ENGINE = GraphiteMergeTree('graphite_rollup')
--         PARTITION BY toYYYYMM(date)
--         ORDER BY (metric, timestamp)
--         SETTINGS index_granularity = 8192;

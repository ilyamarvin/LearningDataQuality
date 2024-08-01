CREATE DATABASE dq;

CREATE TABLE default.assembly_task_issued
(
    `rid` String,
    `shk_id` Int64,
    `chrt_id` UInt32,
    `nm_id` UInt32,
    `as_id` UInt32,
    `wh_id` UInt16,
    `issued_dt` DateTime,
    `entry` LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toStartOfWeek(issued_dt, 1)
ORDER BY shk_id
TTL toStartOfWeek(issued_dt, 1) + toIntervalWeek(14)
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 72000, ttl_only_drop_parts = 1;

CREATE TABLE dq.assembly_task_issued engine = ReplacingMergeTree order by dt_h as
    select toStartOfHour(issued_dt) dt_h
         , count(shk_id) qty_shk
         , uniq(shk_id) uniq_shk
         , uniq(rid) uniq_rid
         , countIf(shk_id, chrt_id = 0) qty_empty_chrt_id
         , countIf(shk_id, nm_id = 0) qty_empty_nm_id
         , uniq(as_id) qty_as
         , countIf(shk_id, wh_id = 0) qty_empty_wh_id
    from assembly_task_issued
    group by dt_h;
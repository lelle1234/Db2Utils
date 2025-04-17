#!/bin/bash

########################################################################### 
# Simple index evaluator that examines the powerset of suggested indexes
# for the query, and choose the best subset for each subset cardinality.
# 
# The user can then choose one of these sub sets, and expose the ddl and
# estimated size for the indexes in this sub set. 
#
# In addition it offers a sneak preview of the access plan should those
# indexes be created.
#
# Author: Lennart Jonsson, Castra Norr AB
###########################################################################

usage() {
    echo
    echo "Usage: $(basename $0) -d <db> -f <sqlfile> [-s <schema>] [-u <user>] [-p <pwd>] [-c]"
    echo
    echo "                        -d <db>           : which database to use"
    echo "                        -f <sqlfile>      : file containing query"
    echo "                        -s <schema>       : optional, default schema"
    echo "                        -u <user>         : optional, user that should connect to db"
    echo "                        -p <pwd>          : optional, password for user"
    echo "                        -c                : optional, drop and create explain/advise tables in systools schema"
    echo 
    exit 1
}

create=0
OPTS=$(getopt ":d:f:s:u:p:c" "$@")
eval set -- "$OPTS"
while true ; do
    case "$1" in
        -d) db="$2"; shift 2;;
        -f) qf="$2"; shift 2;;
        -s) schema="$2"; shift 2;;
        -u) user="$2"; shift 2;;
        -p) passwd="$2"; shift 2;;
        -c) create=1; shift;;
        --) shift; break;;
    esac
done

[ -z ${db+x} ] && usage
[ -z ${qf+x} ] && usage

if [[ -v user ]]; then
    db2 connect to "${db}" user "${user}" using "${passwd}" > /dev/null 2>&1
else
    db2 connect to "${db}" > /dev/null 2>&1
fi

if [ $? -ne 0 ]; then
    echo "Unable to connect to ${db}"
    exit 1
fi

if [ ! -e "${qf}" ]; then
    echo "File ${qf} does not exist"
    exit 1
fi

if [[ -v schema ]]; then
    db2 +c "set current_schema ${schema}" > /dev/null 2>&1
fi

if [ "${create}" -eq 1 ]; then
    db2 +c "CALL sysproc.sysinstallobjects('EXPLAIN', 'D', cast (null as varchar(128)), 'SYSTOOLS')" > /dev/null 2>&1
    db2 +c "CALL sysproc.sysinstallobjects('EXPLAIN', 'C', cast (null as varchar(128)), 'SYSTOOLS')" > /dev/null 2>&1
fi

# clean up
db2 +c "DELETE FROM systools.advise_index"  > /dev/null 2>&1
db2 +c "DROP TABLE IF EXISTS costtbl"  > /dev/null 2>&1
db2 +c "CREATE TABLE costtbl
    ( nr int not null primary key
    , indexes varchar(4096) not null
    , total_cost decimal(20,4) not null
    , io_cost decimal(20,4) not null
    , cpu_cost decimal(20,4) not null
)"  > /dev/null 2>&1


# find suspects
db2 +c -x "SET current explain mode recommend indexes" > /dev/null 2>&1
db2 +c -x -tf "${qf}" > /dev/null 2>&1
if [ $? -ge 4 ]; then
    echo
    echo "Unable to evaluate query. Possible causes: you need to specify default schema and/or create explain tables:"
    echo
    echo "                              $(basename $0) -d ${db} ... -s <schema> -c"
    echo
    db2 rollback > /dev/null 2>&1
    exit 1
fi
db2 +c -x "SET current explain mode no" > /dev/null 2>&1

unset indexes
for i in $(db2 +c -x "SELECT DISTINCT rtrim(name) FROM systools.advise_index WHERE exists = 'N'"); do
    indexes+=("$i ")
done

# number of indexes
n=${#indexes[@]}

# size of powerset of indexes
powersize=$((1 << n))

echo
i=0
while [ "${i}" -lt "${powersize}" ]
do
    # progressbar
    printf "\rEvaluating solution: %${n}d [%d]" "${i}" "$((powersize - 1))"

    # disable all indexes
    db2 +c -x "UPDATE systools.advise_index SET use_index = 'N'" > /dev/null 2>&1

    # current subset of indexes
    subset=()
    j=0
    while [ "${j}" -lt "${n}" ]
    do
        if [ $(((1 << j) & i)) -gt 0 ]
        then
            subset+=("${indexes[$j]}")
        fi
        j=$((j + 1))
    done

    # enable indexes in current subset

    for ind in "${subset[@]}"; do
        ix=$(echo "${ind}" | awk '{$1=$1};1')
        db2 +c -x "UPDATE systools.advise_index SET use_index = 'Y' WHERE name = '${ix}'" > /dev/null 2>&1
    done

    # evaluate solution and store cost
    db2 +c -x "SET current explain mode evaluate indexes" > /dev/null 2>&1
    db2 +c -x -tf "${qf}" > /dev/null 2>&1
    db2 +c -x "SET current explain mode no" > /dev/null 2>&1

    db2 +c -x "INSERT INTO costtbl (nr, indexes, total_cost, io_cost, cpu_cost)
                SELECT ${i}, '${subset[*]}', dec(total_cost,20,4), dec(io_cost,20,4), dec(cpu_cost,20,4)
                FROM systools.Explain_Operator a
                WHERE a.operator_type = 'RETURN'
                ORDER BY a.explain_time desc
                FETCH FIRST 1 ROWS ONLY
                WITH ur" > /dev/null 2>&1

    i=$((i + 1))
done
echo


# calulate max length
len=0
for ix in "${indexes[@]}"; do
    len=$((len + ${#ix}))
done

# Display cheapest solution for each subset cardinality
db2 +c "SELECT row_number() over (order by length(indexes))::smallint - 1 as nr
     , SUBSTR(indexes, 1, ${len}+${n}) as index_combination
     , int(total_cost) as tot_cost, int(io_cost) as io_cost, bigint(cpu_cost/1000) as cpu_cost
     , CASE WHEN max_cost <> total_cost
            THEN decimal(100.0*(max_cost - total_cost) / max_cost,5,2)
       END as tot_cost_improvement_pct
FROM (
    SELECT indexes, total_cost, io_cost, cpu_cost
         , (SELECT total_cost FROM costtbl WHERE indexes = '') as max_cost
         , ROW_NUMBER() OVER (PARTITION BY length(indexes) ORDER BY total_cost) as rn
    FROM costtbl
)
WHERE rn = 1
ORDER BY length(indexes), total_cost desc"

echo
min=0
max=$n
while true; do
    read -r -p "Which combination would you like to examine (${min}-${max})? " m
    if ! [[ $m =~ ^[0-9]+$ ]]; then
        echo "Only numbers allowed"
        continue
    fi
    if (( m < min || m > max )); then
        echo "Wrong input, use a number between ${min} and ${max}"
        continue
    fi
    break
done
    

if [ "${m}" -eq 0 ]; then
    echo "Bye"
    echo
    db2 rollback > /dev/null 2>&1
    exit 0
fi

# reset all indexes
db2 +c -x "UPDATE systools.advise_index SET use_index = 'N'"

# Output ddl for chosen sub-set
echo
for ix in $(db2 -x +c "select index_combination from (
    SELECT row_number() over (order by length(indexes)) - 1 as nr
        , substr(indexes, 1, ${len}+${n}) as index_combination
        FROM (
            SELECT indexes, total_cost
            , (SELECT total_cost from costtbl where indexes = '') as max_cost
            , row_number() over (partition by length(indexes) order by total_cost) as rn
        FROM costtbl
    )
    WHERE rn = 1
) WHERE nr = ${m} order by nr"); do
    db2 +c -x "SELECT distinct cast(creation_text as varchar(2000)) 
                        || CHR(10) || 'COLLECT SAMPLED DETAILED STATISTICS' 
                        || CHR(10) || 'COMPRESS YES;'
               FROM systools.advise_index WHERE name = '${ix}'" |\
        sed -e "s/\"//g" -e "s/ [ ]*/ /g" -e "s/[ ]\././g" -e "s/ALLOW REVERSE SCANS/\nALLOW REVERSE SCANS/"

    echo

    db2 +c -x "UPDATE systools.advise_index SET use_index = 'Y' WHERE name = '${ix}'" > /dev/null 2>&1

    # approximate size of index, use rcte to extract set of column's from colnames string 
    db2 +c -x "WITH split (tabschema, tabname, remaining_str, colname, n) as (
                SELECT distinct rtrim(ai.tbcreator), rtrim(ai.tbname), substr(ai.colnames::varchar(4000), 2)
                    , regexp_substr(ai.colnames::varchar(4000), '[^+-]+', 2, 1)::varchar(128), 0
                FROM systools.advise_index ai
                WHERE name = '${ix}'
                UNION ALL
                SELECT tabschema, tabname,
                    CASE WHEN instr(remaining_str, '+') > 0 and instr(remaining_str, '-') > 0 
                         THEN substr(remaining_str, least(nullif(instr(remaining_str, '+'), 0), nullif(instr(remaining_str, '-'), 0)) + 1)
                         WHEN instr(remaining_str, '+') > 0 then substr(remaining_str, instr(remaining_str, '+') + 1)
                         WHEN instr(remaining_str, '-') > 0 then substr(remaining_str, instr(remaining_str, '-') + 1)
                         ELSE null
                    END,
                    regexp_substr(remaining_str, '[^+-]+', 1, 1),
                    n+1
                FROM split
                WHERE remaining_str IS NOT null and length(remaining_str) > 0 and n<100
            )
            SELECT 'Estimated Size (Mb): ', (sum(length * card * 1.3) / 1024 / 1024) as estimatedindexsize_mb
            FROM (
                SELECT distinct c.colname, c.length, t.card
                FROM split s
                JOIN syscat.columns c
                    USING (tabschema, tabname, colname)
                JOIN syscat.tables t
                    USING (tabschema, tabname)
                WHERE colname IS NOT null
            )"

    echo
    echo

done

read -r -p "Show plan with chosen indexes? (y/n) " m
if [ "${m}" = "y" ]; then

    # evaluate chosen set of indexes once more
    db2 +c -x "SET current explain mode evaluate indexes" > /dev/null 2>&1
    db2 +c -x -tf "${qf}" > /dev/null 2>&1
    db2 +c -x "SET current explain mode no" > /dev/null 2>&1

    # Credit to Marcus Winand, https://use-the-index-luke.com/sql/explain-plan/db2/getting-an-execution-plan
    db2 +c -x "CREATE OR REPLACE VIEW last_explained AS
                WITH tree(operator_ID, level, path, explain_time, cycle) AS
                ( SELECT 1 operator_id 
                        , 0 level
                        , CAST('001' AS VARCHAR(1000)) path
                        , max(explain_time) explain_time
                        , 0
                  FROM SYSTOOLS.EXPLAIN_OPERATOR O
                  WHERE O.EXPLAIN_REQUESTER = SESSION_USER

                  UNION ALL

                  SELECT s.source_id
                        , level + 1
                        , tree.path || '/' || LPAD(CAST(s.source_id AS VARCHAR(3)), 3, '0')  path
                        , tree.explain_time
                        , POSITION('/' || LPAD(CAST(s.source_id AS VARCHAR(3)), 3, '0')  || '/' IN path USING OCTETS)
                  FROM tree
                    , SYSTOOLS.EXPLAIN_STREAM S
                  WHERE s.target_id    = tree.operator_id
                    AND s.explain_time = tree.explain_time
                    AND S.Object_Name IS NULL
                    AND S.explain_requester = SESSION_USER
                    AND tree.cycle = 0
                    AND level < 100
                )
                SELECT * 
                FROM (
                    SELECT Explain_Plan
                    FROM (
                        SELECT CAST(   LPAD(id,        MAX(LENGTH(id))        OVER(), ' ')
                                || ' | ' 
                                || RPAD(operation, MAX(LENGTH(operation)) OVER(), ' ')
                                || ' | ' 
                                || LPAD(rows,      MAX(LENGTH(rows))      OVER(), ' ')
                                || ' | ' 
                                -- Don't show ActualRows columns if there are no actuals available at all 
                                || CASE WHEN COUNT(ActualRows) OVER () > 1 -- the heading 'ActualRows' is always present, so 1 means no OTHER values
                                        THEN LPAD(ActualRows, MAX(LENGTH(ActualRows)) OVER(), ' ') || ' | ' 
                                        ELSE ''
                                   END
                                || LPAD(cost,      MAX(LENGTH(cost))      OVER(), ' ') AS VARCHAR(100)) Explain_Plan
                                , path
                        FROM (
                            SELECT 'ID' ID
                                , 'Operation' Operation
                                , 'Rows' Rows
                                , 'ActualRows' ActualRows
                                , 'Cost' Cost
                                , '0' Path
                            FROM SYSIBM.SYSDUMMY1
                            UNION
                            SELECT CAST(tree.operator_id as VARCHAR(254)) ID
                                 , CAST(LPAD(' ', tree.level, ' ')
                                || CASE WHEN tree.cycle = 1
                                        THEN '(cycle) '
                                        ELSE ''
                                   END     
                                || COALESCE (TRIM(O.Operator_Type) || COALESCE(' (' || argument || ')', '') || ' ' || COALESCE(S.Object_Name,'') , '') AS VARCHAR(254)) AS OPERATION
                                , COALESCE(CAST(rows AS VARCHAR(254)), '') Rows
                                , CAST(ActualRows as VARCHAR(254)) ActualRows -- note: no coalesce
                                , COALESCE(CAST(CAST(O.Total_Cost AS BIGINT) AS VARCHAR(254)), '') Cost
                                , path
                        FROM tree
                        LEFT JOIN ( 
                            SELECT i.source_id
                                , i.target_id
                                , CAST(CAST(ROUND(o.stream_count) AS BIGINT) AS VARCHAR(12)) || ' of '
                                || CAST (total_rows AS VARCHAR(12))
                                || CASE WHEN total_rows > 0
                                            AND ROUND(o.stream_count) <= total_rows 
                                        THEN ' (' || LPAD(CAST (ROUND(ROUND(o.stream_count)/total_rows*100,2) AS NUMERIC(5,2)), 6, ' ') || '%)'
                                        ELSE ''
                                   END rows
                                ,  CASE WHEN act.actual_value is not null 
                                        THEN CAST(CAST(ROUND(act.actual_value) AS BIGINT) AS VARCHAR(12)) || ' of ' || CAST (total_rows AS VARCHAR(12))
                                    || CASE WHEN total_rows > 0 
                                            THEN ' (' || LPAD(CAST (ROUND(ROUND(act.actual_value)/total_rows*100,2) AS NUMERIC(5,2)), 6, ' ') || '%)'
                                            ELSE NULL
                                        END 
                                    END ActualRows
                                , i.object_name
                                , i.explain_time
                            FROM (
                                SELECT MAX(source_id) source_id
                                    , target_id
                                    , MIN(CAST(ROUND(stream_count,0) AS BIGINT)) total_rows
                                    , CAST(LISTAGG(object_name) AS VARCHAR(50)) object_name
                                    , explain_time
                                FROM SYSTOOLS.EXPLAIN_STREAM
                                WHERE explain_time = (SELECT MAX(explain_time)
                                                      FROM SYSTOOLS.EXPLAIN_OPERATOR
                                                      WHERE EXPLAIN_REQUESTER = SESSION_USER
                                                     )
                                GROUP BY target_id, explain_time
                            ) I
                            LEFT JOIN SYSTOOLS.EXPLAIN_STREAM O
                                ON (    I.target_id=o.source_id
                                AND I.explain_time = o.explain_time
                                AND O.EXPLAIN_REQUESTER = SESSION_USER
                                )
                            LEFT JOIN SYSTOOLS.EXPLAIN_ACTUALS act
                                ON (    act.operator_id  = i.target_id
                                AND act.explain_time = i.explain_time
                                AND act.explain_requester = SESSION_USER
                                AND act.ACTUAL_TYPE  like 'CARDINALITY%'
                                )
                        ) s
                            ON (    s.target_id    = tree.operator_id
                            AND s.explain_time = tree.explain_time
                    )
                    LEFT JOIN SYSTOOLS.EXPLAIN_OPERATOR O
                        ON (    o.operator_id  = tree.operator_id
                        AND o.explain_time = tree.explain_time
                        AND o.explain_requester = SESSION_USER
                ) 
                LEFT JOIN (
                    SELECT LISTAGG (CASE argument_type
                                    WHEN 'UNIQUE' THEN CASE WHEN argument_value = 'TRUE'
                                                            THEN 'UNIQUE'
                                                            ELSE NULL
                                                       END
                                    WHEN 'TRUNCSRT' THEN CASE WHEN argument_value = 'TRUE'
                                                              THEN 'TOP-N'
                                                              ELSE NULL
                                                          END   
                                    WHEN 'SCANDIR' THEN CASE WHEN argument_value != 'FORWARD'
                                                             THEN argument_value
                                                             ELSE NULL
                                                         END                     
                                    ELSE argument_value     
                             END, ' ') argument
                        , operator_id
                        , explain_time
                    FROM SYSTOOLS.EXPLAIN_ARGUMENT EA
                    WHERE argument_type IN ('AGGMODE'   -- GRPBY
                                         , 'UNIQUE', 'TRUNCSRT' -- SORT
                                         , 'SCANDIR' -- IXSCAN, TBSCAN
                                         , 'OUTERJN' -- JOINs
                                         )
                      AND explain_requester = SESSION_USER
                    GROUP BY explain_time, operator_id

                ) A
                    ON (    a.operator_id  = tree.operator_id
                    AND a.explain_time = tree.explain_time
                    )
            ) O
        )
        ORDER BY path
    )" > /dev/null 2>&1

    db2 +c "select * from last_explained"
fi

db2 rollback > /dev/null 2>&1
db2 terminate > /dev/null 2>&1
exit 0


class DBQueries:

    read_edges = """
        SELECT 'C' as type, T.CONSTNAME, T.TABSCHEMA, T.TABNAME
             , 'T' as BTYPE, 'N/A', T.TABSCHEMA AS BSCHEMA, T.TABNAME AS BNAME
        FROM SYSCAT.TABCONST T
        LEFT JOIN SYSCAT.CHECKS C
            ON (T.CONSTNAME, T.TABSCHEMA, T.TABNAME) = (C.CONSTNAME, C.TABSCHEMA, C.TABNAME)
        WHERE T.TABSCHEMA NOT LIKE 'SYS%'
        -- ignore system generated check constraints
        AND COALESCE (C.TYPE, '-') <> 'S' 
        AND EXISTS (
            SELECT 1 FROM SYSCAT.TABLES T2
            WHERE (T.TABSCHEMA, T.TABNAME) = (T2.TABSCHEMA, T2.TABNAME)
            AND T2.TYPE <> 'N'
        )
        -- AND NOT EXISTS (
        --    SELECT 1 FROM SYSCAT.CHECKS X
        --    WHERE (T.TABSCHEMA, T.TABNAME, T.CONSTNAME) = (X.TABSCHEMA, X.TABNAME, X.CONSTNAME)
        --    -- AND X.CONSTNAME LIKE 'SQL%'
        -- )

        UNION ALL    

        SELECT 'T' as type, 'N/A', T.TABSCHEMA, T.TABNAME
            ,  CASE WHEN R.TABSCHEMA IS NULL THEN 'DUMMIE' ELSE 'T' END, 'N/A'
            ,  COALESCE(R.REFTABSCHEMA, 'DUMMIE'), COALESCE(R.REFTABNAME, 'DUMMIE')
        FROM SYSCAT.TABLES T
        LEFT JOIN SYSCAT.REFERENCES R
            ON (T.TABSCHEMA, T.TABNAME) = (R.TABSCHEMA, R.TABNAME)
        WHERE T.TABSCHEMA NOT LIKE 'SYS%' AND T.TYPE <> 'N'

        UNION ALL

        SELECT 'I', 'N/A', I.INDSCHEMA, I.INDNAME, 'T', 'N/A', I.TABSCHEMA, I.TABNAME
        FROM SYSCAT.INDEXES I
        WHERE I.INDSCHEMA NOT LIKE 'SYS%'
        AND EXISTS (
            SELECT 1 FROM SYSCAT.TABLES T2
            WHERE (I.TABSCHEMA, I.TABNAME) = (T2.TABSCHEMA, T2.TABNAME)
            AND T2.TYPE <> 'N'
        )    

        UNION ALL

        SELECT 'F', R.SPECIFICNAME, R.ROUTINESCHEMA, R.ROUTINENAME, 'DUMMIE', 'N/A', 'DUMMIE', 'DUMMIE'
        FROM SYSCAT.ROUTINES R
        WHERE R.ROUTINESCHEMA NOT LIKE 'SYS%' AND R.ROUTINESCHEMA NOT LIKE 'SQLJ%'

        UNION ALL

        SELECT 'X', 'N/A', D.TRIGSCHEMA, D.TRIGNAME, 'DUMMIE', 'N/A', 'DUMMIE', 'DUMMIE'
        FROM SYSCAT.TRIGDEP D 
        WHERE D.TRIGSCHEMA NOT LIKE 'SYS%'

        UNION ALL

        SELECT 'C' as type, CONSTNAME, TABSCHEMA, TABNAME
            , CASE WHEN BTYPE IN ('T','S','V','N') THEN 'T' ELSE BTYPE END AS BTYPE, 'N/A', BSCHEMA, BNAME 
        FROM SYSCAT.CONSTDEP x
        WHERE TABSCHEMA NOT LIKE 'SYS%' AND BSCHEMA NOT LIKE 'SYS%' AND BTYPE <> 'N'
        AND EXISTS (
            SELECT 1 FROM SYSCAT.TABLES T
            WHERE (X.TABSCHEMA, X.TABNAME) = (T.TABSCHEMA, T.TABNAME)
            AND T.TYPE <> 'N'
        ) AND BTYPE <> 'F'    

        UNION ALL

        SELECT 'I', 'N/A', D.INDSCHEMA, D.INDNAME 
             , CASE WHEN D.BTYPE IN ('T','S','V','N') THEN 'T' ELSE D.BTYPE END, 'N/A', D.BSCHEMA, D.BNAME 
        FROM SYSCAT.INDEXDEP D
        JOIN SYSCAT.INDEXES I
            ON D.INDSCHEMA = I.INDSCHEMA AND D.INDNAME = I.INDNAME 
        WHERE D.INDSCHEMA NOT LIKE 'SYS%'

        UNION ALL

        SELECT 'F', R1.SPECIFICNAME, R1.ROUTINESCHEMA, R1.ROUTINENAME
            , CASE WHEN D.BTYPE IN ('T','S','V','N') THEN 'T' ELSE D.BTYPE END
            , COALESCE(R2.SPECIFICNAME, 'N/A')
            , COALESCE(R2.ROUTINESCHEMA, D.BSCHEMA), COALESCE(R2.ROUTINENAME, D.BNAME) 
        FROM SYSCAT.ROUTINEDEP D
        JOIN SYSCAT.ROUTINES R1
            ON D.ROUTINESCHEMA = R1.ROUTINESCHEMA AND D.SPECIFICNAME = R1.SPECIFICNAME 
        LEFT JOIN SYSCAT.ROUTINES R2 
            ON D.BSCHEMA = R2.ROUTINESCHEMA AND D.BNAME = R2.SPECIFICNAME AND D.BTYPE = 'F' 
        WHERE D.ROUTINESCHEMA NOT LIKE 'SYS%' 
        AND D.BSCHEMA NOT LIKE 'SYS%' 
        AND D.BTYPE <> 'K'
        AND D.ROUTINESCHEMA NOT LIKE 'SQLJ%'

        UNION ALL

        SELECT 'T', 'N/A', TABSCHEMA, TABNAME
            , CASE WHEN BTYPE IN ('T','S','V','N') THEN 'T' ELSE BTYPE END, 'N/A', BSCHEMA, BNAME
        FROM SYSCAT.TABDEP
        WHERE TABSCHEMA NOT LIKE 'SYS%' AND BSCHEMA NOT LIKE 'SYS%'

        UNION ALL

        SELECT 'X', 'N/A', D.TRIGSCHEMA, D.TRIGNAME
            , CASE WHEN D.BTYPE IN ('T','S','V','N') THEN 'T' ELSE D.BTYPE END
            , COALESCE(R.SPECIFICNAME, 'N/A'), BSCHEMA, COALESCE(R.ROUTINENAME, D.BNAME) 
        FROM SYSCAT.TRIGDEP D 
        LEFT JOIN SYSCAT.ROUTINES R 
            ON D.BSCHEMA = R.ROUTINESCHEMA AND D.BNAME = R.SPECIFICNAME
        WHERE TRIGSCHEMA NOT LIKE 'SYS%' AND BSCHEMA NOT LIKE 'SYS%'

        UNION ALL

        SELECT 'X', 'N/A', T.TRIGSCHEMA, T.TRIGNAME
              , 'T', 'N/A', T.TABSCHEMA, T.TABNAME
        FROM SYSCAT.TRIGGERS T
        WHERE TRIGSCHEMA NOT LIKE 'SYS%'

        UNION ALL

        SELECT 'T', 'N/A', TABSCHEMA, TABNAME, 'T', 'N/A', REFTABSCHEMA, REFTABNAME
        FROM SYSCAT.REFERENCES
        WHERE TABSCHEMA NOT LIKE 'SYS%'

        UNION ALL

        SELECT 'C', CONSTNAME, TABSCHEMA, TABNAME, 'C', REFKEYNAME, REFTABSCHEMA, REFTABNAME
        FROM SYSCAT.REFERENCES
        WHERE REFTABSCHEMA NOT LIKE 'SYS%' AND TABSCHEMA NOT LIKE 'SYS%'
        ORDER BY 3,4 """

    read_tables = """select rtrim(tabschema), rtrim(tabname), compression, tbspace
                          , index_tbspace, long_tbspace, tableorg, volatile, append_mode, remarks
                     from syscat.tables
                     where type = 'T'
                     and tabschema not like 'SYS%'
                     and tabschema not like 'SQL%'
                     """

    read_views = """select rtrim(t.tabschema), rtrim(t.tabname), 
                           substr(t.property,13,1) as query_opt, v.text, t.type, t.remarks
                    from syscat.tables t
                    join syscat.views v
                        on t.tabschema = v.viewschema
                        and t.tabname = v.viewname
                    where t.type in ('S', 'V')
                 """

    read_tab_columns = """select rtrim(tabschema), rtrim(tabname), rtrim(colname), typename
                            , length/case coalesce(typestringunits,'') when 'CODEUNITS32' then 4 else 1 end as length
                            , scale
                            , nulls, default, generated, identity, text, compress, inline_length, remarks
                            , codepage
                      from syscat.columns
                      where tabschema not like 'SYS%' 
                      and tabschema not like 'SQL%' 
                      order by tabschema, tabname, colno
                      """

    read_indexes = """select rtrim(i.tabschema), rtrim(i.tabname), rtrim(i.indschema), rtrim(i.indname)
                            , i.uniquerule, i.indextype, i.pctfree, i.reverse_scans, i.compression, i.nullkeys
                            , i.remarks, x.typemodel, rtrim(x.datatype), x.length, x.scale, x.pattern
                      from syscat.indexes i
                      left join syscat.indexxmlpatterns x
                        on i.indschema = x.indschema
                        and i.indname = x.indname
                      where i.indschema not like 'SYS%'
                      and i.indschema not like 'SQL%'
                      """

    read_index_columns = """select rtrim(indschema), rtrim(indname), rtrim(colname), colorder
                            from syscat.indexcoluse
                            where indschema not like 'SYS%'
                            and indschema not like 'SQL%'
                            order by indschema, indname, colseq
                            """

    read_routines = """select rtrim(specificname), rtrim(routineschema), rtrim(routinename), text, routinetype, remarks 
                       from syscat.routines 
                       where routineschema not like 'SYS%' 
                       and routineschema not like 'SQL%' 
                       and origin = 'Q'
                       """

    read_external_routines = """select rtrim(r.specificname)
                                    , rtrim(r.routineschema), rtrim(r.routinename)
                                    , r.routinetype 
                                    , p.typename
                                    , p.length
                                    , p.scale
                                    , r.language
                                    , r.parameter_style
                                    , r.deterministic
                                    , r.external_action
                                    , r.fenced
                                    , r.threadsafe
                                    , r.class || '!' || substr(implementation,1,locate('(',implementation)) || ')'
                                    , r.remarks
                                    , p.codepage
                                    , r.sql_data_access
                                from syscat.routines r
                                join syscat.routineparms p
                                    on r.routineschema = p.routineschema
                                    and r.specificname = p.specificname
                                    and p.ordinal = 0
                                where r.routineschema not like 'SYS%' 
                                and r.routineschema not like 'SQL%' 
                                and origin = 'E'
                                """

    read_external_routine_parms = """select rtrim(p.specificname), rtrim(p.routineschema), rtrim(p.routinename)
                                            , p.parmname, p.typename, p.length, p.scale, p.codepage
                                      from syscat.routineparms p
                                      join syscat.routines r   
                                        on r.routineschema = p.routineschema
                                        and r.specificname = p.specificname 
                                      where p.routineschema not like 'SYS%' 
                                        and p.routineschema not like 'SQL%' 
                                        and p.ordinal > 0 
                                        and r.origin = 'E'
                                  """

    read_foreign_keys = """select rtrim(c.constname), rtrim(c.tabschema), rtrim(c.tabname)
                                , c.type, c.enforced, c.trusted, c.enablequeryopt, deleterule, updaterule 
                                , rtrim(reftabschema), rtrim(reftabname)
                                , rtrim(listagg(k1.colname, ', ') within group (order by k1.colseq)) as colnames
                                , rtrim(listagg(k2.colname, ', ') within group (order by k2.colseq)) as refcolnames
                                , c.remarks
                          from syscat.tabconst c 
                          join syscat.references r 
                            on (rtrim(c.constname), rtrim(c.tabschema), rtrim(c.tabname)) 
                             = (rtrim(r.constname), rtrim(r.tabschema), rtrim(r.tabname))
                          join syscat.keycoluse k1
                            on (r.constname, r.tabschema, r.tabname) = (k1.constname, k1.tabschema, k1.tabname)
                         join syscat.keycoluse k2
                            on (r.refkeyname, r.reftabschema, r.reftabname) = (k2.constname, k2.tabschema, k2.tabname)  
                            and k1.colseq = k2.colseq
                          where c.constname not like 'SYS%'
                       --     and c.constname not like 'SQL%'
                         group by  rtrim(c.constname), rtrim(c.tabschema), rtrim(c.tabname)
                                 , c.type, c.enforced, c.trusted, c.enablequeryopt, deleterule, updaterule
                                 , reftabschema, reftabname, c.remarks 
                        """

    read_candidate_keys = """select rtrim(c.constname), rtrim(c.tabschema), rtrim(c.tabname)
                          , c.type, c.enforced, c.trusted, c.enablequeryopt
                          , listagg(k.colname, ', ') within group (order by colseq) as colnames, c.remarks
                     from syscat.tabconst c
                     join syscat.keycoluse k
                        on (c.constname, c.tabschema, c.tabname) = (k.constname, k.tabschema, k.tabname)
                        and c.constname not like 'SYS%'
                     where c.type in ('P','U')
                     group by rtrim(c.constname), rtrim(c.tabschema), rtrim(c.tabname)
                          , c.type, c.enforced, c.trusted, c.enablequeryopt, c.remarks
                   """

    read_checks = """select rtrim(c.constname), rtrim(c.tabschema), rtrim(c.tabname)
                          , c.enforced, c.trusted, c.enablequeryopt, cc.text, c.remarks
                     from syscat.tabconst c
                     join syscat.checks cc
                        on (c.constname, c.tabschema, c.tabname) = (cc.constname, cc.tabschema, cc.tabname)
                     --   and c.type = 'K'
                        -- ignore system-generated constraints 
                        and cc.type <> 'S'
                     --   and c.constname not like 'SYS%'
                  """

# and c.constname not like 'SQL%'

    read_triggers = """select rtrim(tabschema), rtrim(tabname), rtrim(trigschema), rtrim(trigname), text, remarks
                     from syscat.triggers
                     where tabschema not like 'SYS%'
                     and tabschema not like 'SQL%'
                    """

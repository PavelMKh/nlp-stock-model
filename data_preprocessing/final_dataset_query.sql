SELECT 
    r.ticker,
    r.publication_date,
    r.report_date, 
    r.report_type, 
    r.full_content, 
    q_before.close AS close_day_before,
    q_after_1.close AS close_day_after_1,
    q_after_3.close AS close_day_after_3,
    q_after_10.close AS close_day_after_10,
    q_after_30.close AS close_day_after_30
FROM 
    reports r
LEFT JOIN (
    SELECT 
        q.ticker,
        q.close,
        q.quote_date
    FROM 
        quotes q
) q_before ON r.ticker = q_before.ticker 
            AND q_before.quote_date = (
                SELECT MAX(q2.quote_date)
                FROM quotes q2
                WHERE q2.ticker = r.ticker 
                  AND q2.quote_date < r.publication_date
            )
LEFT JOIN (
    SELECT 
        q.ticker,
        q.close,
        q.quote_date
    FROM 
        quotes q
) q_after_1 ON r.ticker = q_after_1.ticker 
              AND q_after_1.quote_date = (
                  SELECT MIN(q3.quote_date)
                  FROM quotes q3
                  WHERE q3.ticker = r.ticker 
                    AND q3.quote_date > r.publication_date
              )
LEFT JOIN (
    SELECT 
        q.ticker,
        q.close,
        q.quote_date
    FROM 
        quotes q
) q_after_3 ON r.ticker = q_after_3.ticker 
              AND q_after_3.quote_date = (
                  SELECT MIN(q4.quote_date)
                  FROM quotes q4
                  WHERE q4.ticker = r.ticker 
                    AND q4.quote_date > (r.publication_date + INTERVAL '3 days')
              )
LEFT JOIN (
    SELECT 
        q.ticker,
        q.close,
        q.quote_date
    FROM 
        quotes q
) q_after_10 ON r.ticker = q_after_10.ticker 
               AND q_after_10.quote_date = (
                   SELECT MIN(q5.quote_date)
                   FROM quotes q5
                   WHERE q5.ticker = r.ticker 
                     AND q5.quote_date > (r.publication_date + INTERVAL '10 days')
               )
LEFT JOIN (
    SELECT 
        q.ticker,
        q.close,
        q.quote_date
    FROM 
        quotes q
) q_after_30 ON r.ticker = q_after_30.ticker 
               AND q_after_30.quote_date = (
                   SELECT MIN(q6.quote_date)
                   FROM quotes q6
                   WHERE q6.ticker = r.ticker 
                     AND q6.quote_date > (r.publication_date + INTERVAL '30 days')
               )
ORDER BY 
    r.publication_date;
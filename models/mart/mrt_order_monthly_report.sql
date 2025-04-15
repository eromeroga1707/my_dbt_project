WITH monthly_recap AS (
    SELECT 
        DATE_TRUNC(order_created_at,month) AS order_month,
        COUNT(DISTINCT user_id) AS total_monthly_users,
        COUNT(order_id) AS total_monthly_orders
    FROM {{ ref('int_sales_database__order') }}
    GROUP BY order_month
),

total_monthly_user_from_jawa_timur AS (
    SELECT 
        DATE_TRUNC(order_created_at,month) AS order_month,
        COUNT(DISTINCT o.user_id) AS total_monthly_users_from_jawa_timur
    FROM {{ ref('int_sales_database__order') }} AS o
    LEFT JOIN {{ ref('int_sales_database__user_favorite_product') }} AS u 
        ON o.user_id = u.user_id
    WHERE o.user_state LIKE '%JAWA%TIMUR%'
    GROUP BY order_month
)
SELECT 
    r.order_month,
    COALESCE(r.total_monthly_users, 0) AS total_monthly_users,
    COALESCE(jt.total_monthly_users_from_jawa_timur, 0) AS total_monthly_users_from_jawa_timur,
    COALESCE(r.total_monthly_orders, 0) AS total_monthly_orders
FROM monthly_recap AS r
LEFT JOIN total_monthly_user_from_jawa_timur AS jt 
    ON jt.order_month = r.order_month
ORDER BY r.order_month
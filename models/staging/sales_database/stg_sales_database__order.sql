select
    order_id as _order_id,
    user_name as _user_id,
    order_status as _order_status,
    DATETIME(order_date, "Europe/Paris") AS _order_created_at,
    DATETIME(order_approved_date, "Europe/Paris") AS _order_approved_at,
    DATETIME(pickup_date, "Europe/Paris") AS _picked_up_at,
    DATETIME(delivered_date, "Europe/Paris") AS _delivered_at,
    DATETIME(estimated_time_delivery, "Europe/Paris") AS _estimated_time_delivery
from {{ source('sales_database', 'order') }}
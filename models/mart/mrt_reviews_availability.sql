{{
config(
materialized='table',
tag='hourly'
)
}}
with listings as (
select
neighbourhood_cleansed,
room_type,
number_of_reviews,
availability_365
from {{ ref('stg_airbnb__listings') }}
),
reviews_availability as (
select
neighbourhood_cleansed,
room_type,
sum(number_of_reviews) as total_reviews,
round(avg(availability_365,2 as avg_availability
from listings
group by 1, 2
)
select * from reviews_availability
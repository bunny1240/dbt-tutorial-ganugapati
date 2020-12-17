Select ID,
ORDERID order_id,
PAYMENTMETHOD,
STATUS,
AMOUNT/100 AMOUNT,
CREATED,
_BATCHED_AT
FROM {{ source('stripe', 'payment') }}

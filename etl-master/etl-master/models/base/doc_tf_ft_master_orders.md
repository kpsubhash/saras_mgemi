{% docs doc_tf_ft_master_orders %}


This is the original ft_master_orders table which is a line level table that the data science team used and which powered looker before Shopify was introduced. sst_master_orders_analytics is meant to be a replacement for this table.

This table pulled data from demandware and JaggedPeak/Edge, Square - and later, Predictspring, as well as providing information on transactions/payments and attempting to link original -> exchange order line items to assist with tracking exchnage metrics.

raw_sales_order_items and raw_sales_orders are the mother tables which everything joins onto. These are coming from Jaggedpeak data. From there, we also join raw-event_orders in a few places in different ways in order to get data from these demandware tables such as billing and shipping data, some order identifiers, and a few other quantities.

We join raw_quiet_returns to get return information from Quiet Logistics.

raw_demandware_orders - a jsonb - is joined to get payment data. 

raw_predictspring_order_items is joined to get data from PS - POS system which replaced Square - in order to fill critical data gaps such as customer email (note the coalesce in billing_email).

raw_quiet_shipments - Quiet Logistics data similar to raw_quiet_returns - is joined to get data such as ship date.

ft_exchange_loopup is a recursive loopup table that attempts to link an order line item with any exchange order line item that might exist. In addition, vw_exchange_revenie_patch_bk is used as part of this process to help with reporting properly on revenue and financials for the exchange orders that are identified via ft_exchange_loopup.

The final table is used to grab proper disocunt information for source=JaggedPeak orders.

The filters in the WHERE clause serve the following functions:

a) filter out Jaggedpeak orders with a note indicating it was from the new Shopify website (referencefield3=Shopify) because this data is already being captured via our shopify webhooks and subsequent transformation tables. Without this filter, each order would appear in our final table - sst_master_orders_analytics - twice.

b) order status name <> cancelled removes cancelled orders from demandware orders info

NOTE: planner internal orders (eg transfers from warehouse to retail stores, marketing orders beings ent out for influencers, etc) DO APPEAR IN THIS TABLE with source=Planner. the type_ field is then useful for figuring out the reason for the internal planner order.

{% enddocs %}

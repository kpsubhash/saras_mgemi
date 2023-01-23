{% docs doc_sst_master_orders_analytics %}


This table is a denormalized line item level (NOTE: each LINE can have QUANTITY > 1 - this has implications for how data is joined to this table) sales table that also contains other information often used in business reporting and needs such as returns and exchange/shopnow data. The progenitor to this table was called ft_master_orders.

{% enddocs %}

#ETL
#customer_segment
c_segment=data.frame(segment_name=unique(df1$Customer.Segment))
c_segment$segment_id=1:nrow(c_segment)
c_segment_id_list <- sapply(df1$Customer.Segment, function(x) c_segment$segment_id[c_segment$segment_name == x])
df1$segment_id=c_segment_id_list
dbWriteTable(con1, name="customer_segment", value=c_segment, row.names=FALSE, append=TRUE)
#customer_country
c_country=data.frame(country_name=unique(df1$Customer.Country))
c_country$country_id=1:nrow(c_country)
c_country_id_list <- sapply(df1$Customer.Country, function(x) c_country$country_id[c_country$country_name == x])
df1$country_id=c_country_id_list
dbWriteTable(con1, name="customer_country", value=c_country, row.names=FALSE, append=TRUE)
#transaction_type
tran_type=data.frame(transaction_type=unique(df1$Type))
tran_type$transaction_type_id=1:nrow(tran_type)
tran_type_id_list <- sapply(df1$Type, function(x) tran_type$transaction_type_id[tran_type$transaction_type == x])
df1$transaction_type_id=tran_type_id_list
dbWriteTable(con1, name="transaction_type", value=tran_type, row.names=FALSE, append=TRUE)
#order_status
o_status=data.frame(order_status=unique(df1$Order.Status))
o_status$order_status_id=1:nrow(o_status)
o_status_id_list <- sapply(df1$Order.Status, function(x) o_status$order_status_id[o_status$order_status == x])
df1$order_status_id=o_status_id_list
dbWriteTable(con1, name="order_status", value=o_status, row.names=FALSE, append=TRUE)
#order_region
df1$market_region=paste(df1$Market,df1$Order.Region)
o_region=data.frame(market_region=unique(paste(df1$Market,df1$Order.Region)))
o_region$market_region_id=1:nrow(o_region)
o_region_id_list <- sapply(df1$market_region, function(x) o_region$market_region_id[o_region$market_region == x])
df1$market_region_id=o_region_id_list
dbWriteTable(con1, name="order_region", value=o_region, row.names=FALSE, append=TRUE)
#delivery_status
df1$delivery_status_risk=paste(df1$Delivery.Status,df1$Late_delivery_risk)
s_delivery=data.frame(delivery_status_risk=unique(paste(df1$Delivery.Status,df1$Late_delivery_risk)))
s_delivery$delivery_status_id=1:nrow(s_delivery)
s_delivery_id_list <- sapply(df1$delivery_status_risk, function(x) s_delivery$delivery_status_id[s_delivery$delivery_status_risk == x])
df1$delivery_status_id=s_delivery_id_list
dbWriteTable(con1, name="delivery_status", value=s_delivery, row.names=FALSE, append=TRUE)
#shipping_mode
s_mode=data.frame(shipping_mode=unique(df1$Shipping.Mode))
s_mode$shipping_mode_id=1:nrow(s_mode)
s_mode_id_list <- sapply(df1$Shipping.Mode, function(x) s_mode$shipping_mode_id[s_mode$shipping_mode == x])
df1$shipping_mode_id=s_mode_id_list
dbWriteTable(con1, name="shipping_mode", value=s_mode, row.names=FALSE, append=TRUE)
#category
p_category=data.frame(category_name=unique(df1$Category.Name))
p_category$category_id=1:nrow(p_category)
p_category_id_list <- sapply(df1$Category.Name, function(x) p_category$category_id[p_category$category_name == x])
df1$category_id=p_category_id_list
dbWriteTable(con1, name="category", value=p_category, row.names=FALSE, append=TRUE)
#customer_order
dbWriteTable(con1, name="customer_order", 
             value=df1[c('order_id', 'customer_id','transaction_type_id')][!duplicated(df1[c('order_id', 'customer_id','transaction_type_id')]),], 
             row.names=FALSE,append=TRUE)
#customers
dbWriteTable(con1, name="customers", 
             value=df1[c('customer_id','customer_fname','customer_lname','segment_id','country_id','customer_state','customer_street','customer_zipcode')][!duplicated(df1[c('customer_id','customer_fname','customer_lname','segment_id','country_id','customer_state','customer_street','customer_zipcode')]),], 
             row.names=FALSE, append=TRUE)
#order_item
dbWriteTable(con1, name="order_item", 
             value=df1[c('order_id','order_item_id','product_card_id','item_quantity','item_discount','discount_rate','pre_discount_sales','after_discount_sales','profit_ratio','profit')][!duplicated(df1[c('order_id','order_item_id','product_card_id','item_quantity','item_discount','discount_rate','pre_discount_sales','after_discount_sales','profit_ratio','profit')]),], 
             row.names=FALSE, append=TRUE)
#order-shipping
dbWriteTable(con1, name="order_shipping", 
             value=df1[c('order_id','shipping_date','shipping_mode_id','shipping_days_real','shipping_days_sche','delivery_status_id')][!duplicated(df1[c('order_id','shipping_date','shipping_mode_id','shipping_days_real','shipping_days_sche','delivery_status_id')]),], 
             row.names=FALSE, append=TRUE)
#orders
dbWriteTable(con1, name="orders", 
             value=df1[c('order_id','order_date','market_region_id','order_city','order_state','order_country','order_zipcode','order_status_id')][!duplicated(df1[c('order_id','order_date','market_region_id','order_city','order_state','order_country','order_zipcode','order_status_id')]),], 
             row.names=FALSE, append=TRUE)
#products
dbWriteTable(con1, name="products", 
             value=df1[c('product_card_id','category_id','product_image','product_name','product_price')][!duplicated(df1[c('product_card_id','category_id','product_image','product_name','product_price')]),], 
             row.names=FALSE, overwrite=TRUE)



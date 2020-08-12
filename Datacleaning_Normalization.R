df=read.csv('DataCoSupplyChainDataset.csv')
dim(df)
summary(df)
df=subset(df,select = -c(Customer.Email,Customer.Password))
head(df)
#'Order.Customer.Id','Order.Item.Id','Order.Item.Cardprod.Id','Category.Id'
df[duplicated(df[c('Customer.Fname','Customer.Lname','Customer.Id')]),]
df[duplicated(df$Customer.Id),]
str(df[df$Product.Name %in% c('Smart Watch'),])

#data cleaning
df1=df[df$Product.Status==0,]
dim(df1)
str(df1)
#remove useless columns
df1=df1[,-c(12,16,23:24,51)]
write.csv(df1,'new_df1.csv')
df1=read.csv('new_df2.csv')
#rename database
library(dplyr)
names(df1)[names(df1)=="Customer.Id"]="customer_id"
names(df1)[names(df1)=="Customer.Fname"]= "customer_fname"
names(df1)[names(df1)=="Customer.Lname"]="customer_lname"
names(df1)[names(df1)=="Customer.State"]="customer_state"
names(df1)[names(df1)=="Customer.Street"]="customer_street"
names(df1)[names(df1)=="Customer.Zipcode"]="customer_zipcode"
names(df1)[names(df1)=="Order.Id"]="order_id"
names(df1)[names(df1)=="order.date..DateOrders."]="order_date"
names(df1)[names(df1)=="Order.City"]="order_city"
names(df1)[names(df1)=="Order.State"]="order_state"
names(df1)[names(df1)=="Order.Country"]="order_country"
names(df1)[names(df1)=="Order.Zipcode"]="order_zipcode"
names(df1)[names(df1)=="Product.Card.Id"]="product_card_id"      
names(df1)[names(df1)=="Product.Image"]="product_image"
names(df1)[names(df1)=="Product.Name"]="product_name"
names(df1)[names(df1)=="Product.Price"]="product_price"
names(df1)[names(df1)=="shipping.date..DateOrders."]="shipping_date"
names(df1)[names(df1)=="Days.for.shipping..real."]="shipping_days_real"
names(df1)[names(df1)=="Days.for.shipment..scheduled."]="shipping_days_sche"
names(df1)[names(df1)=="Order.Item.Id"]="order_item_id"
names(df1)[names(df1)=="Order.Item.Quantity"]="item_quantity"
names(df1)[names(df1)=="Order.Item.Discount"]="item_discount"
names(df1)[names(df1)=="Order.Item.Discount.Rate"]="discount_rate"
names(df1)[names(df1)=="Sales"]="pre_discount_sales"
names(df1)[names(df1)=="Order.Item.Total"]="after_discount_sales"
names(df1)[names(df1)=="Order.Item.Profit.Ratio"]="profit_ratio"
names(df1)[names(df1)=="Order.Profit.Per.Order"]="profit"

#SQL
require('RPostgreSQL')
drv <- dbDriver('PostgreSQL')

# Create a connection
con1 <- dbConnect(drv, dbname = 'project1',
                  host = '192.168.99.100', port = 5432,
                  user = 'postgres', password = '123')
con1 <- dbConnect(drv, dbname = 'postgres',
                  host = 'group4.czl0pji2xj8f.us-east-2.rds.amazonaws.com', port = 5432,
                  user = 'postgres', password = 'group400')

# Pass the SQL statements that create all tables
stmt <- " CREATE TABLE  customer_segment(                                                   
		segment_id   integer,
		segment_name  varchar(50),
        PRIMARY KEY (segment_id)
    );

CREATE TABLE  customer_country(                                                   
		country_id   integer,
		country_name  varchar(50),
        PRIMARY KEY (country_id)
    );

CREATE TABLE  customers(                                                   
		customer_id   integer,
		customer_fname  varchar(50),
		customer_lname  varchar(50),
		segment_id integer,
		country_id integer,
		customer_state varchar(50),
		customer_street varchar(100),
		customer_zipcode varchar(30),
        PRIMARY KEY (customer_id),
      FOREIGN KEY(country_id) REFERENCES customer_country(country_id),
      FOREIGN KEY(segment_id) REFERENCES customer_segment(segment_id)
);

CREATE TABLE  transaction_type(                                                   
transaction_type_id   integer,
transaction_type  varchar(50),
        PRIMARY KEY (transaction_type_id)
    );


CREATE TABLE  order_status(                                                   
order_status_id   integer,
order_status  varchar(50),
        PRIMARY KEY (order_status_id)
    );


CREATE TABLE  order_region(     
              market_region_id   integer,      
              market_region      varchar(50),                                
        PRIMARY KEY (market_region_id)
    );


CREATE TABLE orders (
   order_id      integer,
  order_date  timestamp,
  market_region_id   integer,
   order_city      varchar(50),
  order_state    varchar(50),
  order_country   varchar(50),
  order_zipcode    varchar(30),
 order_status_id  integer,
  PRIMARY KEY (order_id),
UNIQUE(order_id),
FOREIGN KEY(market_region_id) REFERENCES order_region,
FOREIGN KEY(order_status_id) REFERENCES order_status
  );



CREATE TABLE  category(                                                   
category_id   integer,
category_name  varchar(50),
        PRIMARY KEY (category_id)
    );

CREATE TABLE  products(     
              product_card_id   integer,      
              category_id     integer,                                
              product_image  varchar(100), 
              product_name    varchar(100),
             product_price     numeric(20,10),
        PRIMARY KEY (product_card_id),
       FOREIGN KEY(category_id) REFERENCES category(category_id));



CREATE TABLE  delivery_status(                                                   
delivery_status_id   integer,
delivery_status_risk  varchar(50),
        PRIMARY KEY (delivery_status_id)
    );
CREATE TABLE  shipping_mode(
shipping_mode_id   integer,
shipping_mode  varchar(50),
        PRIMARY KEY (shipping_mode_id)
    );


CREATE TABLE  order_shipping(                                                   
order_id   integer,
shipping_date  timestamp,
               shipping_mode_id  integer,
               shipping_days_real integer,
              shipping_days_sche  integer,
	delivery_status_id integer,
        PRIMARY KEY (order_id),
 FOREIGN KEY(order_id) REFERENCES orders(order_id),
FOREIGN KEY(shipping_mode_id) REFERENCES shipping_mode(shipping_mode_id),
FOREIGN KEY(delivery_status_id) REFERENCES delivery_status
    );

CREATE TABLE order_item (
  order_id  integer,
  order_item_id  integer,
product_card_id integer,
   item_quantity    integer,
  item_discount   numeric(20,10),
 discount_rate    numeric(3,2),
 pre_discount_sales    numeric(20,10),
 after_discount_sales    numeric(20,10),
 profit_ratio        numeric(20,10),
profit             numeric(20,10),
   PRIMARY KEY (order_item_id),
FOREIGN KEY(order_id) REFERENCES orders(order_id) ,
FOREIGN KEY(product_card_Id) REFERENCES products(product_card_id)
  );


CREATE TABLE customer_order (
   order_id      integer,
 customer_id integer,
transaction_type_id  integer,
  PRIMARY KEY (order_id,customer_id),
FOREIGN KEY(order_id) REFERENCES orders(order_id),
FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
FOREIGN KEY(transaction_type_id) REFERENCES transaction_type(transaction_type_id)
  );
"

# Execute the statement to create tables
dbGetQuery(con1, stmt)
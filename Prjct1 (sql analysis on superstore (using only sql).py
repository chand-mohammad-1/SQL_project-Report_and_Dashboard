#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install ipython-sql psycopg2')


# In[5]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[6]:


get_ipython().run_line_magic('sql', 'postgresql://postgres:12345@localhost:5432/sql_analysis')


# In[7]:


get_ipython().run_cell_magic('sql', 'select * from store', 'limit 1\n')


# In[ ]:


######################################################################


# In[ ]:


##########################################################################################3


# # BASIC LEVEL ANALYSIS

# In[5]:


#Database Size

get_ipython().run_line_magic('sql', "SELECT pg_size_pretty(pg_database_size('sql_analysis'));")


# In[6]:


#Table Size

get_ipython().run_line_magic('sql', "SELECT pg_size_pretty(pg_relation_size('store'));")


# In[54]:


#row count of data 
get_ipython().run_line_magic('sql', 'SELECT COUNT(*) AS Row_Count FROM store')


# In[60]:


#column count of data


# In[48]:


get_ipython().run_cell_magic('sql', 'SELECT COUNT(*) AS column_Count FROM information_schema.columns', "\nWHERE table_name = 'store'\n")


# In[61]:


#Check Dataset Information


# In[62]:


get_ipython().run_cell_magic('sql', 'SELECT *', "FROM INFORMATION_SCHEMA.COLUMNS\nWHERE table_name = 'store'\n")


# In[ ]:


#get column names of store data 


# In[63]:


get_ipython().run_cell_magic('sql', 'select column_name', "from INFORMATION_SCHEMA.COLUMNS\nwhere TABLE_NAME='store'\n")


# In[64]:


#get column names with data type of store data


# In[32]:


get_ipython().run_cell_magic('sql', 'select column_name,data_type', "from INFORMATION_SCHEMA.COLUMNS\nwhere TABLE_NAME='store'\n")


# In[ ]:


# checking null values of store data


# In[67]:


get_ipython().run_cell_magic('sql', 'SELECT * FROM store', "WHERE (select column_name\nfrom INFORMATION_SCHEMA.COLUMNS\nwhere TABLE_NAME='store') IS NULL;\n")


# In[68]:


get_ipython().run_cell_magic('sql', 'SELECT *', 'FROM store\nWHERE order_date IS NULL OR\nsales IS NULL OR\nquantity IS NULL OR\ndiscount IS NULL OR\nprofit IS NULL OR\ndiscount_amount IS NULL OR\nyears IS NULL OR\npostal_code IS NULL OR\nship_date IS NULL OR\nrow_id IS NULL OR\nproduct_id IS NULL OR\ncategory IS NULL OR\nsub_category IS NULL OR\nproduct_name IS NULL OR  \ncustomer_duration IS NULL OR\nreturned_items IS NULL OR\nreturn_reason IS NULL OR\norder_id IS NULL OR\nship_mode IS NULL OR\ncustomer_id IS NULL OR\ncustomer_name IS NULL OR\nsegment IS NULL OR\ncountry IS NULL OR\ncity IS NULL OR\nstates IS NULL OR\nregion IS NULL \n')


# In[ ]:


#Dropping Unnecessary column like Row_ID


# In[70]:


get_ipython().run_line_magic('sql', 'ALTER TABLE "store" DROP COLUMN "row_id";')


# In[71]:


get_ipython().run_line_magic('sql', 'select * from store limit 10')


# In[ ]:


#Check the count of United States


# In[72]:


get_ipython().run_cell_magic('sql', 'select count(*) AS US_Count', "from store \nwhere country = 'United States'\n")


# In[ ]:


#This row isn't important for modeling purposes, but important for auto-generating latitude and longitude on Tableau. So, We won't drop it.


# In[ ]:





# # PRODUCT LEVEL ANALYSIS

# In[ ]:


#What are the unique product categories?


# In[73]:


get_ipython().run_line_magic('sql', 'select distinct (Category) from store')


# In[ ]:


#What is the number of products in each category?


# In[74]:


get_ipython().run_cell_magic('sql', 'SELECT Category, count(*) AS No_of_Products', 'FROM store\nGROUP BY Category\norder by  count(*) desc\n')


# In[ ]:


#Find the number of Subcategories products that are divided.


# In[75]:


get_ipython().run_cell_magic('sql', 'select count(distinct (Sub_Category)) As No_of_Sub_Categories', 'from store\n')


# In[ ]:


#Find the number of products in each sub-category.


# In[76]:


get_ipython().run_cell_magic('sql', 'SELECT Sub_Category, count(*) As No_of_products', 'FROM store\nGROUP BY Sub_Category\norder by  count(*) desc\n')


# In[ ]:


#Find the number of unique product names.


# In[77]:


get_ipython().run_cell_magic('sql', 'select count(distinct (Product_Name)) As No_of_unique_products', 'from store\n')


# In[ ]:


#Which are the Top 10 Products that are ordered frequently?


# In[78]:


get_ipython().run_cell_magic('sql', 'SELECT Product_Name, count(*) AS No_of_products', 'FROM store\nGROUP BY Product_Name\norder by  count(*) desc\nlimit 10\n')


# In[ ]:


#Calculate the cost for each Order_Id with respective Product Name.


# In[79]:


get_ipython().run_cell_magic('sql', 'select Order_Id,Product_Name,ROUND(CAST((sales-profit) AS NUMERIC), 2)as cost', 'from store\n')


# In[ ]:


#Calculate % profit for each Order_ID with respective Product Name.


# In[80]:


get_ipython().run_cell_magic('sql', 'select Order_Id,Product_Name,ROUND(CAST((profit/((sales-profit))*100)AS NUMERIC),2) as percentage_profit', 'from store\n\n')


# #Calculate percentage profit and group by them with Product Name and Order_Id.
# #Introducing method using WITH 

# In[87]:


get_ipython().run_cell_magic('sql', 'WITH store_new as(', 'select a.*,b.percentage_profit\nfrom store as a\nleft join\n(select ((profit/((sales-profit))*100)) as percentage_profit,order_id,Product_Name from store\ngroup by percentage_profit,Product_Name,order_id) as b\non a.order_id=b.order_id)\nselect * from store_new\nlimit 10\n')


# In[ ]:


#Same Thing Using normal method without creating any temporary data. Here, This can be only viewed for one time and we can't merge with the current dataset in this process.


# In[88]:


get_ipython().run_cell_magic('sql', 'select  product_id,order_id,((profit/((sales-profit))*100)) as percentage_profit', 'from store\ngroup by product_id,order_id,percentage_profit\nlimit 10\n')


# In[ ]:





# /* Where can we trim some loses? 
#    In Which products?
#    We can do this by calculating the average sales and profits, and comparing the values to that average.
#    If the sales or profits are below average, then they are not best sellers and 
#    can be analyzed deeper to see if its worth selling thema anymore. */

# In[89]:


get_ipython().run_cell_magic('sql', 'SELECT round(cast(AVG(sales) as numeric),2) AS avg_sales', 'FROM store;\n')


# #the average sales on any given product is 229.8, so approx. 230.

# In[90]:


get_ipython().run_cell_magic('sql', 'SELECT round(cast(AVG(Profit)as numeric),2) AS avg_profit', 'FROM store;\n')


# 
# -- the average profit on any given product is 28.6, or approx 29.

# In[ ]:


#Average sales per sub-cat


# In[91]:


get_ipython().run_cell_magic('sql', 'SELECT round(cast(AVG(sales) as numeric),2) AS avg_sales, Sub_Category', 'FROM store\nGROUP BY Sub_Category\nORDER BY avg_sales asc\nlimit 9;\n')


# --The sales of these Sub_category products are below the average sales.

# In[ ]:


#Average profit per sub-cat


# In[92]:


get_ipython().run_cell_magic('sql', 'SELECT round(cast(AVG(Profit)as numeric),2) AS avg_prof,Sub_Category', 'FROM store\nGROUP BY Sub_Category\nORDER BY avg_prof asc\nlimit 11;\n')


# --The profit of these Sub_category products are below the average profit.
# -- "Minus sign" Respresnts that those products are in losses.

# # CUSTOMER LEVEL ANALYSIS

# In[ ]:


#What is the number of unique customer IDs?


# In[93]:


get_ipython().run_cell_magic('sql', 'select count(distinct (Customer_id)) as no_of_unique_custd_ID', 'from store\n')


# In[ ]:


#Find those customers who registered during 2014-2016.


# In[94]:


get_ipython().run_cell_magic('sql', 'select distinct  (Customer_ID), Order_ID,city, Postal_Code', 'from store\nwhere Customer_Id is not null;\n')


# In[ ]:


#Calculate Total Frequency of each order id by each customer Name in descending order.


# In[95]:


get_ipython().run_cell_magic('sql', 'select order_id, customer_id, count(Order_Id) as total_order_id', 'from store\ngroup by order_id,customer_id\norder by total_order_id desc\n')


# In[ ]:


#Calculate  cost of each customer name.


# In[96]:


get_ipython().run_cell_magic('sql', 'select order_id, customer_id, City, Quantity,sales,(sales-profit) as costs,profit', 'from store\ngroup by order_id,customer_id,City,Quantity,Costs,sales,profit;\n')


# In[ ]:


#Display No of Customers in each region in descending order.


# In[97]:


get_ipython().run_cell_magic('sql', 'select Region, count(*) as No_of_Customers', 'from store\ngroup by region\norder by no_of_customers desc\n')


# In[ ]:


#Find Top 10 customers who order frequently.


# In[98]:


get_ipython().run_cell_magic('sql', 'SELECT Customer_Name, count(*) as no_of_order', 'FROM store\nGROUP BY Customer_Name\norder by  count(*) desc\nlimit 10\n')


# In[ ]:


#Find Top 20 Customers who benefitted the store.


# In[99]:


get_ipython().run_cell_magic('sql', 'SELECT Customer_Name, Profit, City, States', 'FROM store\nGROUP BY Customer_Name,Profit,City,States\norder by  Profit desc\nlimit 20\n')


# In[ ]:


#Which state(s) is the superstore most succesful in? Least?
#Top 10 results:


# In[100]:


get_ipython().run_cell_magic('sql', 'SELECT round(cast(SUM(sales) as numeric),2) AS state_sales, States', 'FROM Store\nGROUP BY States\nORDER BY state_sales DESC\nOFFSET 1 ROWS FETCH NEXT 10 ROWS ONLY;\n')


# # ORDER LEVEL ANALYSIS 

# In[ ]:


#number of unique orders


# In[101]:


get_ipython().run_cell_magic('sql', 'select count(distinct (Order_ID)) as no_of_unique_orders', 'from store\n')


# In[ ]:


#Find Sum Total Sales of Superstore.


# In[102]:


get_ipython().run_cell_magic('sql', 'select round(cast(SUM(sales) as numeric),2) as Total_Sales', 'from store\n')


# In[ ]:


#Calculate the time taken for an order to ship and converting the no. of days in int format.


# In[103]:


get_ipython().run_cell_magic('sql', 'select order_id,customer_id,customer_name,city,states, (ship_date-order_date) as delivery_duration', 'from store\norder by delivery_duration desc\nlimit 20\n')


# In[ ]:


#Extract the year  for respective order ID and Customer ID with quantity.


# In[104]:


get_ipython().run_cell_magic('sql', 'select order_id,customer_id,quantity,EXTRACT(YEAR from Order_Date)', 'from store\ngroup by order_id,customer_id,quantity,EXTRACT(YEAR from Order_Date) \norder by quantity desc\n')


# In[ ]:


#What is the Sales impact?


# In[105]:


get_ipython().run_cell_magic('sql', 'SELECT EXTRACT(YEAR from Order_Date), Sales, round(cast(((profit/((sales-profit))*100))as numeric),2) as profit_percentage', 'FROM store\nGROUP BY EXTRACT(YEAR from Order_Date), Sales, profit_percentage\norder by  profit_percentage \nlimit 20\n')


# In[ ]:


#####################################


# --Breakdown by Top vs Worst Sellers:

# In[ ]:


#Find Top 10 Categories (with the addition of best sub-category within the category).


# In[106]:


get_ipython().run_cell_magic('sql', 'SELECT  Category, Sub_Category , round(cast(SUM(sales) as numeric),2) AS prod_sales', 'FROM store\nGROUP BY Category,Sub_Category\nORDER BY prod_sales DESC;\n')


# In[ ]:


#Find Top 10 Sub-Categories. :


# In[107]:


get_ipython().run_cell_magic('sql', 'SELECT round(cast(SUM(sales) as numeric),2) AS prod_sales,Sub_Category', 'FROM store\nGROUP BY Sub_Category\nORDER BY prod_sales DESC\nOFFSET 1 ROWS FETCH NEXT 10 ROWS ONLY;\n')


# In[ ]:


#Find Worst 10 Categories.:


# In[108]:


get_ipython().run_cell_magic('sql', 'SELECT round(cast(SUM(sales) as numeric),2) AS prod_sales, Category, Sub_Category', 'FROM store\nGROUP BY Category, Sub_Category\nORDER BY prod_sales;\n')


# In[ ]:


#Find Worst 10 Sub-Categories. :


# In[109]:


get_ipython().run_cell_magic('sql', 'SELECT round(cast(SUM(sales) as numeric),2) AS prod_sales, sub_Category', 'FROM store\nGROUP BY Sub_Category\nORDER BY prod_sales\nOFFSET 1 ROWS FETCH NEXT 10 ROWS ONLY;\n')


# In[ ]:


#Show the Basic Order information.


# In[110]:


get_ipython().run_cell_magic('sql', 'select count(Order_ID) as Purchases,', 'round(cast(sum(Sales)as numeric),2) as Total_Sales,\nround(cast(sum(((profit/((sales-profit))*100)))/ count(*)as numeric),2) as avg_percentage_profit,\nmin(Order_date) as first_purchase_date,\nmax(Order_date) as Latest_purchase_date,\ncount(distinct(Product_Name)) as Products_Purchased,\ncount(distinct(City)) as Location_count\nfrom store\n')


# In[ ]:


###########################


# # RETURN LEVEL ANALYSIS

# In[ ]:


#Find the number of returned orders.


# In[111]:


get_ipython().run_cell_magic('sql', 'select Returned_items, count(Returned_items)as Returned_Items_Count', "from store\ngroup by Returned_items\nHaving Returned_items='Returned'\n")


# In[ ]:


#Find Top 10 Returned Categories.:


# In[112]:


get_ipython().run_cell_magic('sql', 'SELECT Returned_items, Count(Returned_items) as no_of_returned ,Category, Sub_Category', "FROM store\nGROUP BY Returned_items,Category,Sub_Category\nHaving Returned_items='Returned'\nORDER BY count(Returned_items) DESC\nlimit 10;\n")


# In[ ]:


#Find Top 10  Returned Sub-Categories.:


# In[113]:


get_ipython().run_cell_magic('sql', 'SELECT Returned_items, Count(Returned_items),Sub_Category', "FROM store\nGROUP BY Returned_items, Sub_Category\nHaving Returned_items='Returned'\nORDER BY Count(Returned_items) DESC\nOFFSET 1 ROWS FETCH NEXT 10 ROWS ONLY;\n")


# In[ ]:


#Find Top 10 Customers Returned Frequently.:


# In[114]:


get_ipython().run_cell_magic('sql', 'SELECT Returned_items, Count(Returned_items) As Returned_Items_Count, Customer_ID,Customer_duration, States,City', "FROM store\nGROUP BY Returned_items,customer_name, Customer_ID,customer_duration,states,city\nHaving Returned_items='Returned'\nORDER BY Count(Returned_items) DESC\nlimit 10;\n")


# In[ ]:


#Find Top 20 cities and states having higher return.


# In[115]:


get_ipython().run_cell_magic('sql', 'SELECT Returned_items, Count(Returned_items)as Returned_Items_Count,States,City', "FROM store\nGROUP BY Returned_items,states,city\nHaving Returned_items='Returned'\nORDER BY Count(Returned_items) DESC\nlimit 20;\n")


# In[ ]:


#Check whether new customers are returning higher or not.


# In[116]:


get_ipython().run_cell_magic('sql', 'SELECT Returned_items, Count(Returned_items)as Returned_Items_Count,Customer_duration', "FROM store\nGROUP BY Returned_items,Customer_duration\nHaving Returned_items='Returned'\nORDER BY Count(Returned_items) DESC\nlimit 20;\n")


# In[ ]:


#Find Top  Reasons for returning.


# In[117]:


get_ipython().run_cell_magic('sql', 'SELECT Returned_items, Count(Returned_items)as Returned_Items_Count,return_reason', "FROM store\nGROUP BY Returned_items,return_reason\nHaving Returned_items='Returned'\nORDER BY Count(Returned_items) DESC\n")


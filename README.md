# Whats included 
The repo is to supplement the [YouTube Channel](https://www.youtube.com/@starttech5697)
 on Mastering AWS Glue. It includes a cloudformation template which creates the s3 bucket, glue tables, IAM roles, and csv data files.

# Overview
1.  Spin up resources using cloudformation template
2.  Add csv files to S3 bucket in relative folder location
3.  Create Glue notebook
4.  Execute PySpark code

# Data
Below are the schemas for the tables created in the Glue Data Catalog by the cloudformation template. They also include a small sample of data to aid the explanation of the coding syntax.

### Customers

|Customerid | Firstname | Lastname | Fullname |
|-----------|----------|----------|-----------|
|293        |	Catherine |	Abel                 |	Catherine Abel  |
| 295       |	Kim	      |   Abercrombie        |	Kim Abercrombie |
|  297      |	Humberto  |	Acevedo              |	Humberto Acevedo|
  



### Orders

|SalesOrderID |SalesOrderDetailID |OrderDate |DueDate |ShipDate |EmployeeID |CustomerID |SubTotal |TaxAmt |Freight |TotalDue |ProductID |OrderQty |UnitPrice |UnitPriceDiscount |LineTotal |
|-------------|-------------------|----------|--------|---------|-----------|-----------|---------|-------|--------|---------|----------|---------|----------|------------------|----------|
|71782 |110667 |5/1/2014 |5/13/2014 |5/8/2014 |276 |293 |33319.986 |3182.8264 |994.6333 |37497.4457 |714 |3 |29.994 |0 |89.982 |
|44110 |1732 |8/1/2011 |8/13/2011 |8/8/2011 |277 |295 |16667.3077 |1600.6864 |500.2145 |18768.2086 |765 |2 |419.4589 |0 |838.9178 |
|44131 |2005 |8/1/2011 |8/13/2011 |8/8/2011 |275 |297 |20514.2859 |1966.5222 |614.5382 |23095.3463 |709 |6 |5.7 |0 |34.2 |

### Employees

|EmployeeID |ManagerID |FirstName |LastName |FullName |JobTitle |OrganizationLevel |MaritalStatus |Gender |Territory |Country |Group|
|-----------|----------|----------|--------|---------|-----------|-----------|---------------------|-------|--------|---------|----------|
|276 |274 |Linda |Mitchell |Linda Mitchell |Sales Representative |3 |M |F |Southwest |US |North America|
|277 |274 |Jillian |Carson |Jillian Carson |Sales Representative |3 |S |F |Central |US |North America|
|275 |274 |Michael |Blythe |Michael Blythe |Sales Representative |3 |S |M	|Northeast	|US	|North America|


# Main tutorial

1. Create Resources using supplied cloud formation template see video

2. Upload csv folder/files to S3 bucket see video

3. Create Glue Notebook see videox

4. Read data from Customers Table using Notebook Using Dynamic Frame
<pre>
    <code>
         # Read from the customers table in the glue data catalog using a dynamic frame
        dynamicFrameCustomers = glueContext.create_dynamic_frame.from_catalog(
       database = "pyspark_tutorial_db", 
       table_name = "customers"
       )
     # Show the top 10 rows from the dynamic dataframe
     dynamicFrameCustomers.show(10)
    </code>
</pre>

5. Check data types in Dynamic Frame
<pre>
    <code>
# Check types in dynamic frame
dynamicFrameCustomers.printSchema()
    </code>
</pre>

6. Count The Number of Rows in a Dynamic DataFrame, select,drop fields from dynamic frame and top 10 rows

<pre>
    <code>
          # Count The Number of Rows in a Dynamic Dataframe 
          dynamicFrameCustomers.count()
          # Selecting certain fields from a Dynamic DataFrame
          dyfCustomerSelectFields = dynamicFrameCustomers.select_fields(["customerid", "fullname"])
          # Show Top 10 rows of dyfCustomerDropFields Dynamic Frame
          dynamicFrameCustomers.show(10)
         #Drop Fields of Dynamic Frame
         dyfCustomerDropFields = dynamicFrameCustomers.drop_fields(["firstname","lastname"])
         # Show Top 10 rows of dyfCustomerDropFields Dynamic Frame
         dyfCustomerDropFields.show(10)
     </code>
</pre>

7. Rename Columns in a Dynamic Frame

<pre>
    <code>
			# Mapping array for column rename fullname -> name
			mapping=[("customerid", "long", "customerid","long"),("fullname", "string", "name", "string")]

			# Apply the mapping to rename fullname -> name
			dfyMapping = ApplyMapping.apply(
                                frame = dyfCustomerDropFields, 
                                mappings = mapping, 
                                transformation_ctx = "applymapping1"
                                )

			# show the new dynamic frame with name column 
				dfyMapping.show(10)
      </code>
</pre>
     
8. Filter data in a Dynamic Frame

<pre>
    <code>
				# Filter dynamicFrameCustomers for customers who have the last name Adams
				dyfFilter=  Filter.apply(frame = dynamicFrameCustomers, 
														f = lambda x: x["lastname"] in "Adams"
													)

				# Show the top 10 customers  from the filtered Dynamic frame 
				dyfFilter.show(10)
				
     </code>
</pre>


9. Join Two Dynamic frames on a equality join

<pre>
    <code>
#read up orders dynamic frame
# Read from the customers table in the glue data catalog using a dynamic frame
dynamicFrameOrders = glueContext.create_dynamic_frame.from_catalog(
database = "pyspark_tutorial_db", 
table_name = "orders"
)

# show top 10 rows of orders table
dynamicFrameOrders.show(10)
#join customers and orders dynamic frame
# Join two dynamic frames on an equality join
dyfjoin = dynamicFrameCustomers.join(["customerid"],["customerid"],dynamicFrameOrders)

# show top 10 rows for the joined dynamic 
dyfjoin.show(10)
     </code>
</pre>

10. Write Down Data from a Dynamic Frame To S3

<pre>
    <code>
#Create a folder in the S3 bucket created by Cloudformation to use as a location to write the data down to write_down_dyf_to_s3
#Write down data to S3 using the dynamic DataFrame writer class for an S3 path.
# write down the data in a Dynamic Frame to S3 location. 
glueContext.write_dynamic_frame.from_options(
                        frame = dynamicFrameCustomers,
                        connection_type="s3", 
                        connection_options = {"path": "s3://<YOUR_S£_BUCKET_NAME>/write_down_dyf_to_s3"}, 
                        format = "csv", 
                        format_options={
                            "separator": ","
                            },
                        transformation_ctx = "datasink2")
						
						     </code>
</pre>


11. Join Two Dynamic frames on a equality join

<pre>
    <code>
#read up orders dynamic frame
# Read from the customers table in the glue data catalog using a dynamic frame
dynamicFrameOrders = glueContext.create_dynamic_frame.from_catalog(
database = "pyspark_tutorial_db", 
table_name = "orders"
)

# show top 10 rows of orders table
dynamicFrameOrders.show(10)
#join customers and orders dynamic frame
# Join two dynamic frames on an equality join
dyfjoin = dynamicFrameCustomers.join(["customerid"],["customerid"],dynamicFrameOrders)

# show top 10 rows for the joined dynamic 
dyfjoin.show(10)
	    						     </code>
</pre>




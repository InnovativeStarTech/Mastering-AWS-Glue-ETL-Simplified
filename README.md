# Data
Below are the schemas for the tables created in the Glue Data Catalog by the cloudformation template. They also include a small sample of data to aid the explanation of the coding syntax.

### Customers

<table>
  
  Customerid	Firstname	Lastname	Fullname
  293	Catherine	Abel	Catherine Abel
  295	Kim	Abercrombie	Kim Abercrombie
  297	Humberto	Acevedo	Humberto Acevedo 
  
</table>

### Orders

SalesOrderID	SalesOrderDetailID	OrderDate	DueDate	ShipDate	EmployeeID	CustomerID	SubTotal	TaxAmt	Freight	TotalDue	ProductID	OrderQty	UnitPrice	UnitPriceDiscount	LineTotal
71782	110667	5/1/2014	5/13/2014	5/8/2014	276	293	33319.986	3182.8264	994.6333	37497.4457	714	3	29.994	0	89.982
44110	1732	8/1/2011	8/13/2011	8/8/2011	277	295	16667.3077	1600.6864	500.2145	18768.2086	765	2	419.4589	0	838.9178
44131	2005	8/1/2011	8/13/2011	8/8/2011	275	297	20514.2859	1966.5222	614.5382	23095.3463	709	6	5.7	0	34.2
### Employees

EmployeeID	ManagerID	FirstName	LastName	FullName	JobTitle	OrganizationLevel	MaritalStatus	Gender	Territory	Country	Group
276	274	Linda	Mitchell	Linda Mitchell	Sales Representative	3	M	F	Southwest	US	North America
277	274	Jillian	Carson	Jillian Carson	Sales Representative	3	S	F	Central	US	North America
275	274	Michael	Blythe	Michael Blythe	Sales Representative	3	S

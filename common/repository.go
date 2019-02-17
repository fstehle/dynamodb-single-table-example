package common

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"strconv"
)

const (
	categoryPrefix = "categories"
	customerPrefix = "customers"
	employeePrefix = "employees"
	productPrefix  = "products"
	shipperPrefix  = "shippers"
	supplierPrefix = "suppliers"
)

// Internal records for DynamoDB
type dynamodDbRecord struct {
	Pk   string `dynamodbav:"pk,omitempty"`
	Sk   string `dynamodbav:"sk,omitempty"`
	Data string `dynamodbav:"data,omitempty"`
}

type DynamoDBCategory struct {
	CategoryID   int    `dynamodbav:"categoryID,omitempty"`
	CategoryName string `dynamodbav:"categoryName,omitempty"`
	Description  string `dynamodbav:"description,omitempty"`
	Picture      string `dynamodbav:"picture,omitempty"`
}

type DynamoDBCustomer struct {
	CustomerID   string `dynamodbav:"customerID,omitempty"`
	CompanyName  string `dynamodbav:"companyName,omitempty"`
	ContactName  string `dynamodbav:"contactName,omitempty"`
	ContactTitle string `dynamodbav:"contactTitle,omitempty"`
	Address      string `dynamodbav:"address,omitempty"`
	City         string `dynamodbav:"city,omitempty"`
	Region       string `dynamodbav:"region,omitempty"`
	PostalCode   string `dynamodbav:"postalCode,omitempty"`
	Country      string `dynamodbav:"country,omitempty"`
	Phone        string `dynamodbav:"phone,omitempty"`
	Fax          string `dynamodbav:"fax,omitempty"`
}

type DynamoDBEmployee struct {
	EmployeeID      int    `dynamodbav:"employeeID,omitempty"`
	LastName        string `dynamodbav:"lastName,omitempty"`
	FirstName       string `dynamodbav:"firstName,omitempty"`
	Title           string `dynamodbav:"title,omitempty"`
	TitleOfCourtesy string `dynamodbav:"titleOfCourtesy,omitempty"`
	BirthDate       string `dynamodbav:"birthDate,omitempty"`
	HireDate        string `dynamodbav:"hireDate,omitempty"`
	Address         string `dynamodbav:"address,omitempty"`
	City            string `dynamodbav:"city,omitempty"`
	Region          string `dynamodbav:"region,omitempty"`
	PostalCode      string `dynamodbav:"postalCode,omitempty"`
	Country         string `dynamodbav:"country,omitempty"`
	HomePhone       string `dynamodbav:"homePhone,omitempty"`
	Extension       string `dynamodbav:"extension,omitempty"`
	Photo           string `dynamodbav:"photo,omitempty"`
	Notes           string `dynamodbav:"notes,omitempty"`
	ReportsTo       string `dynamodbav:"reportsTo,omitempty"`
	PhotoPath       string `dynamodbav:"photoPath,omitempty"`
}

type DynamoDBOrderDetail struct {
	OrderID   int    `dynamodbav:"orderID,omitempty"`
	ProductID int    `dynamodbav:"productID,omitempty"`
	UnitPrice string `dynamodbav:"unitPrice,omitempty"`
	Quantity  string `dynamodbav:"quantity,omitempty"`
	Discount  string `dynamodbav:"discount,omitempty"`
}

type DynamoDBOrder struct {
	OrderID        int    `dynamodbav:"orderID,omitempty"`
	CustomerID     string `dynamodbav:"customerID,omitempty"`
	EmployeeID     int    `dynamodbav:"employeeID,omitempty"`
	OrderDate      string `dynamodbav:"orderDate,omitempty"`
	RequiredDate   string `dynamodbav:"requiredDate,omitempty"`
	ShippedDate    string `dynamodbav:"shippedDate,omitempty"`
	ShipVia        string `dynamodbav:"shipVia,omitempty"`
	Freight        string `dynamodbav:"freight,omitempty"`
	ShipName       string `dynamodbav:"shipName,omitempty"`
	ShipAddress    string `dynamodbav:"shipAddress,omitempty"`
	ShipCity       string `dynamodbav:"shipCity,omitempty"`
	ShipRegion     string `dynamodbav:"shipRegion,omitempty"`
	ShipPostalCode string `dynamodbav:"shipPostalCode,omitempty"`
	ShipCountry    string `dynamodbav:"shipCountry,omitempty"`
}

type DynamoDBProduct struct {
	ProductID       int    `dynamodbav:"productID,omitempty"`
	ProductName     string `dynamodbav:"productName,omitempty"`
	SupplierID      int    `dynamodbav:"supplierID,omitempty"`
	CategoryID      int    `dynamodbav:"categoryID,omitempty"`
	QuantityPerUnit string `dynamodbav:"quantityPerUnit,omitempty"`
	UnitPrice       string `dynamodbav:"unitPrice,omitempty"`
	UnitsInStock    string `dynamodbav:"unitsInStock,omitempty"`
	UnitsOnOrder    string `dynamodbav:"unitsOnOrder,omitempty"`
	ReorderLevel    string `dynamodbav:"reorderLevel,omitempty"`
	Discontinued    string `dynamodbav:"discontinued,omitempty"`
}

type DynamoDBShipper struct {
	ShipperID   int    `dynamodbav:"shipperID,omitempty"`
	CompanyName string `dynamodbav:"companyName,omitempty"`
	Phone       string `dynamodbav:"phone,omitempty"`
}

type DynamoDBSupplier struct {
	SupplierID   int    `dynamodbav:"supplierID,omitempty"`
	CompanyName  string `dynamodbav:"companyName,omitempty"`
	ContactName  string `dynamodbav:"contactName,omitempty"`
	ContactTitle string `dynamodbav:"contactTitle,omitempty"`
	Address      string `dynamodbav:"address,omitempty"`
	City         string `dynamodbav:"city,omitempty"`
	Region       string `dynamodbav:"region,omitempty"`
	PostalCode   string `dynamodbav:"postalCode,omitempty"`
	Country      string `dynamodbav:"country,omitempty"`
	Phone        string `dynamodbav:"phone,omitempty"`
	Fax          string `dynamodbav:"fax,omitempty"`
	HomePage     string `dynamodbav:"homePage,omitempty"`
}

type Repository struct {
	dynamoDBClient dynamodbiface.DynamoDBAPI
	tableName      string
}

func NewRepository(dynamoDBClient dynamodbiface.DynamoDBAPI, tableName string) *Repository {
	return &Repository{
		dynamoDBClient: dynamoDBClient,
		tableName:      tableName,
	}
}

func MarshalEmployee(employee *Employee) (map[string]*dynamodb.AttributeValue, error) {
	record := &dynamodDbRecord{
		Pk:   strconv.Itoa(employee.EmployeeID),
		Sk:   employee.ReportsTo,
		Data: employee.HireDate,
	}
	attributeValues, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return nil, fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}

	return attributeValues, nil
}

func (r *Repository) StoreCategory(category *Category) error {
	attributeValues, err := dynamodbattribute.MarshalMap(DynamoDBCategory(*category))
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}
	attributeValues["pk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%d", categoryPrefix, category.CategoryID)),
	}
	attributeValues["sk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%s", categoryPrefix, category.CategoryName)),
	}
	attributeValues["data"] = &dynamodb.AttributeValue{
		S: aws.String(category.Description),
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      attributeValues,
	}

	_, err = r.dynamoDBClient.PutItem(putItemInput)
	if err != nil {
		return fmt.Errorf("failed to save record to dynamodb: %v", err)
	}

	return nil
}

func (r *Repository) StoreCustomer(customer *Customer) error {
	attributeValues, err := dynamodbattribute.MarshalMap(DynamoDBCustomer(*customer))
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}
	attributeValues["pk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%d", customerPrefix, customer.CustomerID)),
	}
	attributeValues["sk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s", customer.ContactName)),
	}
	attributeValues["data"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%s#%s#%s", customer.Country, customer.Region, customer.City, customer.Address)),
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      attributeValues,
	}

	_, err = r.dynamoDBClient.PutItem(putItemInput)
	if err != nil {
		return fmt.Errorf("failed to save record to dynamodb: %v", err)
	}

	return nil
}

func (r *Repository) StoreEmployee(employee *Employee) error {
	attributeValues, err := dynamodbattribute.MarshalMap(DynamoDBEmployee(*employee))
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}
	attributeValues["pk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%d", employeePrefix, employee.EmployeeID)),
	}
	attributeValues["sk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%s", employeePrefix, employee.ReportsTo)),
	}
	attributeValues["data"] = &dynamodb.AttributeValue{
		S: aws.String(employee.HireDate),
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      attributeValues,
	}

	_, err = r.dynamoDBClient.PutItem(putItemInput)
	if err != nil {
		return fmt.Errorf("failed to save record to dynamodb: %v", err)
	}

	return nil
}

func (r *Repository) StoreOrderDetail(orderDetail *OrderDetail) error {
	attributeValues, err := dynamodbattribute.MarshalMap(DynamoDBOrderDetail(*orderDetail))
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}
	attributeValues["pk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%d", orderDetail.OrderID)),
	}
	attributeValues["sk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%d", productPrefix, orderDetail.ProductID)),
	}
	attributeValues["data"] = &dynamodb.AttributeValue{
		S: aws.String(orderDetail.UnitPrice),
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      attributeValues,
	}

	_, err = r.dynamoDBClient.PutItem(putItemInput)
	if err != nil {
		return fmt.Errorf("failed to save record to dynamodb: %v", err)
	}

	return nil
}

func (r *Repository) StoreOrder(order *Order) error {
	attributeValues, err := dynamodbattribute.MarshalMap(DynamoDBOrder(*order))
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}
	attributeValues["pk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%d", order.OrderID)),
	}
	attributeValues["sk"] = &dynamodb.AttributeValue{
		S: aws.String("ORDER"),
	}
	attributeValues["data"] = &dynamodb.AttributeValue{
		S: aws.String(order.CustomerID),
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      attributeValues,
	}

	_, err = r.dynamoDBClient.PutItem(putItemInput)
	if err != nil {
		return fmt.Errorf("failed to save record to dynamodb: %v", err)
	}

	return nil
}

func (r *Repository) StoreProduct(product *Product) error {
	attributeValues, err := dynamodbattribute.MarshalMap(DynamoDBProduct(*product))
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}
	attributeValues["pk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%d", productPrefix, product.ProductID)),
	}
	attributeValues["sk"] = &dynamodb.AttributeValue{
		S: aws.String("PRODUCT"),
	}
	if product.Discontinued == "1" {
		attributeValues["data"] = &dynamodb.AttributeValue{
			S: aws.String("1"),
		}
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      attributeValues,
	}

	_, err = r.dynamoDBClient.PutItem(putItemInput)
	if err != nil {
		return fmt.Errorf("failed to save record to dynamodb: %v", err)
	}

	return nil
}

func (r *Repository) StoreShipper(shipper *Shipper) error {
	attributeValues, err := dynamodbattribute.MarshalMap(DynamoDBShipper(*shipper))
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}
	attributeValues["pk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%d", shipperPrefix, shipper.ShipperID)),
	}
	attributeValues["sk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s", shipper.CompanyName)),
	}
	attributeValues["data"] = &dynamodb.AttributeValue{
		S: aws.String(shipper.Phone),
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      attributeValues,
	}

	_, err = r.dynamoDBClient.PutItem(putItemInput)
	if err != nil {
		return fmt.Errorf("failed to save record to dynamodb: %v", err)
	}

	return nil
}

func (r *Repository) StoreSupplier(supplier *Supplier) error {
	attributeValues, err := dynamodbattribute.MarshalMap(DynamoDBSupplier(*supplier))
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record: %v", err)
	}
	attributeValues["pk"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%d", supplierPrefix, supplier.SupplierID)),
	}
	attributeValues["sk"] = &dynamodb.AttributeValue{
		S: aws.String("SUPPLIER"),
	}
	attributeValues["data"] = &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%s#%s#%s#%s", supplier.Country, supplier.Region, supplier.City, supplier.Address)),
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      attributeValues,
	}

	_, err = r.dynamoDBClient.PutItem(putItemInput)
	if err != nil {
		return fmt.Errorf("failed to save record to dynamodb: %v", err)
	}

	return nil
}

// Get employee by employee ID
// table.query(KeyConditionExpression=Key('pk').eq('employees#2'))
func (r *Repository) GetEmployee(employeeID int) (*Employee, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String("pk=:pk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {
				S: aws.String(fmt.Sprintf("%s#%d", employeePrefix, employeeID)),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query employee from dynamodb: %v", err)
	}

	if len(output.Items) == 0 {
		return nil, nil
	}
	record := &DynamoDBEmployee{}
	err = dynamodbattribute.UnmarshalMap(output.Items[0], record)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
	}

	employee := Employee(*record)
	return &employee, nil
}

// Get direct reports for an employee
// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('employees#2'))
func (r *Repository) GetEmployeeDirectReports(employeeID int) ([]*Employee, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("gsi_1"),
		KeyConditionExpression: aws.String("sk=:sk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":sk": {
				S: aws.String(fmt.Sprintf("%s#%d", employeePrefix, employeeID)),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query employee from dynamodb: %v", err)
	}

	var employees []*Employee

	for _, item := range output.Items {
		record := &DynamoDBEmployee{}
		err = dynamodbattribute.UnmarshalMap(item, record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
		}

		employee := Employee(*record)
		employees = append(employees, &employee)
	}

	return employees, nil
}

// Get discontinued products
// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('PRODUCT') & Key('data').eq('1'))
func (r *Repository) GetProductsDiscontinued() ([]*Product, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("gsi_1"),
		KeyConditionExpression: aws.String("sk=:sk AND #data=:data"),
		ExpressionAttributeNames: map[string]*string{
			"#data": aws.String("data"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":sk": {
				S: aws.String("PRODUCT"),
			},
			":data": {
				S: aws.String("1"),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query products from dynamodb: %v", err)
	}

	var products []*Product

	for _, item := range output.Items {
		record := &DynamoDBProduct{}
		err = dynamodbattribute.UnmarshalMap(item, record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
		}

		product := Product(*record)
		products = append(products, &product)
	}

	return products, nil
}

// List all orders of a given product
// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('products#1'))
func (r *Repository) GetOrdersOfProduct(productID int) ([]*Order, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("gsi_1"),
		KeyConditionExpression: aws.String("sk=:sk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":sk": {
				S: aws.String(fmt.Sprintf("%s#%d", productPrefix, productID)),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query orders from dynamodb: %v", err)
	}

	var orders []*Order

	for _, item := range output.Items {
		record := &DynamoDBOrder{}
		err = dynamodbattribute.UnmarshalMap(item, record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
		}

		order := Order(*record)
		orders = append(orders, &order)
	}

	return orders, nil
}

// Get the most recent 25 orders
// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('ORDER'), Limit=25)
func (r *Repository) GetOrdersRecent(limit int64) ([]*Order, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("gsi_1"),
		KeyConditionExpression: aws.String("sk=:sk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":sk": {
				S: aws.String("ORDER"),
			},
		},
		Limit: aws.Int64(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query orders from dynamodb: %v", err)
	}

	var orders []*Order

	for _, item := range output.Items {
		record := &DynamoDBOrder{}
		err = dynamodbattribute.UnmarshalMap(item, record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
		}

		order := Order(*record)
		orders = append(orders, &order)
	}

	return orders, nil
}

// Get shippers by name
// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('United Package'))
func (r *Repository) GetShippersByName(name string) ([]*Shipper, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("gsi_1"),
		KeyConditionExpression: aws.String("sk=:sk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":sk": {
				S: aws.String(name),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query orders from dynamodb: %v", err)
	}

	var shippers []*Shipper

	for _, item := range output.Items {
		record := &DynamoDBShipper{}
		err = dynamodbattribute.UnmarshalMap(item, record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
		}

		shipper := Shipper(*record)
		shippers = append(shippers, &shipper)
	}

	return shippers, nil
}

// Get customers by contact name
// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('Maria Anders'))
func (r *Repository) GetCustomersByContactName(contactName string) ([]*Customer, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("gsi_1"),
		KeyConditionExpression: aws.String("sk=:sk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":sk": {
				S: aws.String(contactName),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query orders from dynamodb: %v", err)
	}

	var customers []*Customer

	for _, item := range output.Items {
		record := &DynamoDBCustomer{}
		err = dynamodbattribute.UnmarshalMap(item, record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
		}

		customer := Customer(*record)
		customers = append(customers, &customer)
	}

	return customers, nil
}

// List all products included in an order
// table.query(KeyConditionExpression=Key('pk').eq('10260') & Key('sk').begins_with('product'))
func (r *Repository) GetProductsInOrder(orderId int) ([]*OrderDetail, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String("pk=:pk AND begins_with(sk,:sk)"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {
				S: aws.String(fmt.Sprintf("%d", orderId)),
			},
			":sk": {
				S: aws.String("product"),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query orders from dynamodb: %v", err)
	}

	var orderDetails []*OrderDetail

	for _, item := range output.Items {
		record := &DynamoDBOrderDetail{}
		err = dynamodbattribute.UnmarshalMap(item, record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
		}

		customer := OrderDetail(*record)
		orderDetails = append(orderDetails, &customer)
	}

	return orderDetails, nil
}

// Get suppliers by country and region
// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('SUPPLIER') & Key('data').begins_with('Germany#NULL'))
func (r *Repository) GetSuppliersByCountry(country string) ([]*Supplier, error) {
	output, err := r.dynamoDBClient.Query(&dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("gsi_1"),
		KeyConditionExpression: aws.String("sk=:sk AND begins_with(#data,:data)"),
		ExpressionAttributeNames: map[string]*string{
			"#data": aws.String("data"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":sk": {
				S: aws.String("SUPPLIER"),
			},
			":data": {
				S: aws.String(country),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query orders from dynamodb: %v", err)
	}

	var suppliers []*Supplier

	for _, item := range output.Items {
		record := &DynamoDBSupplier{}
		err = dynamodbattribute.UnmarshalMap(item, record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record from DynamoDB: %v", err)
		}

		supplier := Supplier(*record)
		suppliers = append(suppliers, &supplier)
	}

	return suppliers, nil
}

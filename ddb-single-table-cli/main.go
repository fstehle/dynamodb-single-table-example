package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/fstehle/dynamodb-single-table-example/common"
	"github.com/fstehle/dynamodb-single-table-example/loader"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
)

var (
	app = kingpin.New("loader", "")

	awsRegion         = app.Flag("aws-region", "aws-region").Default("eu-central-1").String()
	dynamoDBTableName = app.Flag("dynamodb-table-name", "dynamodb-table-name").Default("dynamodb-single-table-example").String()

	createTable               = app.Command("create-table", "Create the dynamoDB table.")
	deleteTable               = app.Command("delete-table", "Delete the dynamoDB table.")
	purgeTable                = app.Command("purge-table", "Remove all the dynamoDB table data.")
	loadTableData             = app.Command("load-table-data", "Load data into the dynamoDB table.")
	loadTableDataCsvDirectory = loadTableData.Flag("csv-directory", "csv-directory").Default("csv").String()
	runQueries                = app.Command("run-queries", "Run some queries within the dynamoDB table.")
)

// Injected with -ldflags
var (
	Version   string
	BuildTime string
)

func main() {
	app.Version(fmt.Sprintf("%v, build time: %v", Version, BuildTime))

	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(*awsRegion),
	}))

	switch command {

	case createTable.FullCommand():
		tableManager := common.NewTableManager(dynamodb.New(sess), *dynamoDBTableName)
		err := tableManager.CreateTable()
		if err != nil {
			log.WithError(err).Fatal("Could not create table")
		}

	case deleteTable.FullCommand():
		tableManager := common.NewTableManager(dynamodb.New(sess), *dynamoDBTableName)
		err := tableManager.DeleteTable()
		if err != nil {
			log.WithError(err).Fatal("Could not delete table")
		}

	case purgeTable.FullCommand():
		tableManager := common.NewTableManager(dynamodb.New(sess), *dynamoDBTableName)
		err := tableManager.PurgeTable()
		if err != nil {
			log.WithError(err).Fatal("Could not purge table")
		}

	case loadTableData.FullCommand():
		repository := common.NewRepository(dynamodb.New(sess), *dynamoDBTableName)
		myLoader := loader.NewLoader(*loadTableDataCsvDirectory, repository)
		err := myLoader.Load()
		if err != nil {
			log.WithError(err).Fatal("error loading data")
		}

	case runQueries.FullCommand():
		repository := common.NewRepository(dynamodb.New(sess), *dynamoDBTableName)
		// a. Get employee by employee ID
		// table.query(KeyConditionExpression=Key('pk').eq('employees#2'))
		employee, err := repository.GetEmployee(2)
		if err != nil {
			log.WithField("employee_id", 2).WithError(err).Fatal("error getting employee")
		}
		log.WithFields(log.Fields{
			"employee_id": employee.EmployeeID,
			"first_name":  employee.FirstName,
			"last_name":   employee.LastName,
		}).Info("Sucessfully retrieved employee data")

		// b. Get direct reports for an employee
		// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('employees#2'))
		directReports, err := repository.GetEmployeeDirectReports(2)
		if err != nil {
			log.WithField("employee_id", 2).WithError(err).Fatal("error getting direct reports for an employee")
		}
		var directReportNames []string
		for _, e := range directReports {
			directReportNames = append(directReportNames, fmt.Sprintf("'%s %s'", e.FirstName, e.LastName))
		}
		log.WithField("direct_report_names", directReportNames).Info("Sucessfully retrieved direct reports for an employee")

		// c. Get discontinued products
		// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('PRODUCT') & Key('data').eq('1'))
		discontinuedProducts, err := repository.GetProductsDiscontinued()
		if err != nil {
			log.WithError(err).Fatal("error getting discontinued products")
		}
		var discontinuedProductNames []string
		for _, e := range discontinuedProducts {
			discontinuedProductNames = append(discontinuedProductNames, fmt.Sprintf("'%s'", e.ProductName))
		}
		log.WithField("discontinued_product_names", discontinuedProductNames).Info("Sucessfully retrieved discontinued products")

		// d. List all orders of a given product
		// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('products#1'))
		orders, err := repository.GetOrdersOfProduct(2)
		if err != nil {
			log.WithField("product_id", 2).WithError(err).Fatal("error getting all orders of a given product")
		}
		var orderIds []int
		for _, o := range orders {
			orderIds = append(orderIds, o.OrderID)
		}
		log.WithFields(log.Fields{
			"product_id": 2,
			"order_ids":  orderIds,
		}).Info("Sucessfully retrieved all orders of a given product")

		// # e. Get the most recent 25 orders
		// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('ORDER'), Limit=25)
		recentOrders, err := repository.GetOrdersRecent(25)
		if err != nil {
			log.WithField("product_id", 2).WithError(err).Fatal("error getting the most recent 25 orders")
		}
		var recentOrderIds []int
		for _, o := range recentOrders {
			recentOrderIds = append(recentOrderIds, o.OrderID)
		}
		log.WithFields(log.Fields{
			"order_ids": recentOrderIds,
		}).Info("Sucessfully retrieved the most recent 25 orders")

		// # f. Get shippers by name
		// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('United Package'))
		shippers, err := repository.GetShippersByName("United Package")
		if err != nil {
			log.WithField("company_name", "United Package").WithError(err).Fatal("error getting shippers by name")
		}
		var shipperIds []int
		for _, s := range shippers {
			shipperIds = append(shipperIds, s.ShipperID)
		}
		log.WithFields(log.Fields{
			"company_name": "United Package",
			"shipper_ids":  shipperIds,
		}).Info("Sucessfully retrieved shippers by name")

		// # g. Get customers by contact name
		// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('Maria Anders'))
		customers, err := repository.GetCustomersByContactName("Maria Anders")
		if err != nil {
			log.WithField("contact_name", "Maria Anders").WithError(err).Fatal("error getting customers by contact name")
		}
		var customerIds []string
		for _, c := range customers {
			customerIds = append(customerIds, c.CustomerID)
		}
		log.WithFields(log.Fields{
			"contact_name": "Maria Anders",
			"customer_ids": customerIds,
		}).Info("Sucessfully retrieved customers by contact name")

		// # h. List all products included in an order
		// table.query(KeyConditionExpression=Key('pk').eq('10260') & Key('sk').begins_with('product'))
		orderDetails, err := repository.GetProductsInOrder(10260)
		if err != nil {
			log.WithField("order_id", 10260).WithError(err).Fatal("error getting all products included in an order")
		}
		var productIds []int
		for _, o := range orderDetails {
			productIds = append(productIds, o.ProductID)
		}
		log.WithFields(log.Fields{
			"order_id":    10260,
			"product_ids": productIds,
		}).Info("Sucessfully retrieved all products included in an order")

		// # i. Get suppliers by country and region
		// table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('SUPPLIER') & Key('data').begins_with('Germany#NULL'))
		suppliers, err := repository.GetSuppliersByCountry("Germany")
		if err != nil {
			log.WithField("country", "Germany").WithError(err).Fatal("error getting suppliers by country and region")
		}
		var supplierCompanyNames []string
		for _, o := range suppliers {
			supplierCompanyNames = append(supplierCompanyNames, fmt.Sprintf("'%s'", o.CompanyName))
		}
		log.WithFields(log.Fields{
			"country":                "Germany",
			"supplier_company_names": supplierCompanyNames,
		}).Info("Sucessfully retrieved suppliers by country and region")
	}

}

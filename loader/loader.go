package loader

import (
	"fmt"
	"github.com/fstehle/dynamodb-single-table-example/common"
	"github.com/gocarina/gocsv"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
)

type Category struct {
	CategoryID   int    `csv:"categoryID"`
	CategoryName string `csv:"categoryName"`
	Description  string `csv:"description"`
	Picture      string `csv:"picture"`
}

type Customer struct {
	CustomerID   string `csv:"customerID"`
	CompanyName  string `csv:"companyName"`
	ContactName  string `csv:"contactName"`
	ContactTitle string `csv:"contactTitle"`
	Address      string `csv:"address"`
	City         string `csv:"city"`
	Region       string `csv:"region"`
	PostalCode   string `csv:"postalCode"`
	Country      string `csv:"country"`
	Phone        string `csv:"phone"`
	Fax          string `csv:"fax"`
}

type Employee struct {
	EmployeeID      int    `csv:"employeeID"`
	LastName        string `csv:"lastName"`
	FirstName       string `csv:"firstName"`
	Title           string `csv:"title"`
	TitleOfCourtesy string `csv:"titleOfCourtesy"`
	BirthDate       string `csv:"birthDate"`
	HireDate        string `csv:"hireDate"`
	Address         string `csv:"address"`
	City            string `csv:"city"`
	Region          string `csv:"region"`
	PostalCode      string `csv:"postalCode"`
	Country         string `csv:"country"`
	HomePhone       string `csv:"homePhone"`
	Extension       string `csv:"extension"`
	Photo           string `csv:"photo"`
	Notes           string `csv:"notes"`
	ReportsTo       string `csv:"reportsTo"`
	PhotoPath       string `csv:"photoPath"`
}

type OrderDetail struct {
	OrderID   int    `csv:"orderID"`
	ProductID int `csv:"productID"`
	UnitPrice string `csv:"unitPrice"`
	Quantity  string `csv:"quantity"`
	Discount  string `csv:"discount"`
}

type Order struct {
	OrderID        int    `csv:"orderID"`
	CustomerID     string `csv:"customerID"`
	EmployeeID     int    `csv:"employeeID"`
	OrderDate      string `csv:"orderDate"`
	RequiredDate   string `csv:"requiredDate"`
	ShippedDate    string `csv:"shippedDate"`
	ShipVia        string `csv:"shipVia"`
	Freight        string `csv:"freight"`
	ShipName       string `csv:"shipName"`
	ShipAddress    string `csv:"shipAddress"`
	ShipCity       string `csv:"shipCity"`
	ShipRegion     string `csv:"shipRegion"`
	ShipPostalCode string `csv:"shipPostalCode"`
	ShipCountry    string `csv:"shipCountry"`
}

type Product struct {
	ProductID       int    `csv:"productID"`
	ProductName     string `csv:"productName"`
	SupplierID      int    `csv:"supplierID"`
	CategoryID      int    `csv:"categoryID"`
	QuantityPerUnit string `csv:"quantityPerUnit"`
	UnitPrice       string `csv:"unitPrice"`
	UnitsInStock    string `csv:"unitsInStock"`
	UnitsOnOrder    string `csv:"unitsOnOrder"`
	ReorderLevel    string `csv:"reorderLevel"`
	Discontinued    string `csv:"discontinued"`
}

type Shipper struct {
	ShipperID   int    `csv:"shipperID"`
	CompanyName string `csv:"companyName"`
	Phone       string `csv:"phone"`
}

type Supplier struct {
	SupplierID   int    `csv:"supplierID"`
	CompanyName  string `csv:"companyName"`
	ContactName  string `csv:"contactName"`
	ContactTitle string `csv:"contactTitle"`
	Address      string `csv:"address"`
	City         string `csv:"city"`
	Region       string `csv:"region"`
	PostalCode   string `csv:"postalCode"`
	Country      string `csv:"country"`
	Phone        string `csv:"phone"`
	Fax          string `csv:"fax"`
	HomePage     string `csv:"homePage"`
}

type LoaderData struct {
	Categories   []*Category
	Customers    []*Customer
	Employees    []*Employee
	OrderDetails []*OrderDetail
	Orders       []*Order
	Products     []*Product
	Shippers     []*Shipper
	Suppliers    []*Supplier
}

type Loader struct {
	csvDirectory string
	repository   *common.Repository
}

func NewLoader(csvDirectory string, repository *common.Repository) *Loader {
	return &Loader{
		csvDirectory: csvDirectory,
		repository:   repository,
	}
}

func (g *Loader) Load() error {
	log.Info("Loading data into the dynamoDB table")

	data, err := g.loadData()
	if err != nil {
		return fmt.Errorf("could not load data: %v", err)
	}

	for _, dataCategory := range data.Categories {
		fmt.Println("Hello category", dataCategory.CategoryName)
		category := common.Category(*dataCategory)
		err := g.repository.StoreCategory(&category)
		if err != nil {
			log.WithError(err).WithField("category_name", category.CategoryName).Errorf("cannot store category")
		}
	}

	for _, dataCustomer := range data.Customers {
		fmt.Println("Hello customer", dataCustomer.CompanyName)
		customer := common.Customer(*dataCustomer)
		err := g.repository.StoreCustomer(&customer)
		if err != nil {
			log.WithError(err).WithField("company_name", customer.CompanyName).Errorf("cannot store customer")
		}
	}

	for _, dataEmployee := range data.Employees {
		fmt.Println("Hello employee", dataEmployee.FirstName)
		employee := common.Employee(*dataEmployee)
		err := g.repository.StoreEmployee(&employee)
		if err != nil {
			log.WithError(err).WithField("first_name", employee.FirstName).Errorf("cannot store employee")
		}
	}

	for _, dataOrderDetail := range data.OrderDetails {
		fmt.Println("Hello order details", dataOrderDetail.OrderID)
		orderDetail := common.OrderDetail(*dataOrderDetail)
		err := g.repository.StoreOrderDetail(&orderDetail)
		if err != nil {
			log.WithError(err).WithField("order_id", orderDetail.OrderID).Errorf("cannot store order detail")
		}
	}

	for _, dataOrder := range data.Orders {
		fmt.Println("Hello order", dataOrder.OrderID)
		order := common.Order(*dataOrder)
		err := g.repository.StoreOrder(&order)
		if err != nil {
			log.WithError(err).WithField("order_id", order.OrderID).Errorf("cannot store order")
		}
	}

	for _, dataProduct := range data.Products {
		fmt.Println("Hello product", dataProduct.ProductName)
		product := common.Product(*dataProduct)
		err := g.repository.StoreProduct(&product)
		if err != nil {
			log.WithError(err).WithField("product_name", product.ProductName).Errorf("cannot store product")
		}
	}

	for _, dataShipper := range data.Shippers {
		fmt.Println("Hello shipper", dataShipper.CompanyName)
		shipper := common.Shipper(*dataShipper)
		err := g.repository.StoreShipper(&shipper)
		if err != nil {
			log.WithError(err).WithField("company_name", shipper.CompanyName).Errorf("cannot store shipper")
		}
	}

	for _, dataSupplier := range data.Suppliers {
		fmt.Println("Hello supplier", dataSupplier.CompanyName)
		supplier := common.Supplier(*dataSupplier)
		err := g.repository.StoreSupplier(&supplier)
		if err != nil {
			log.WithError(err).WithField("company_name", supplier.CompanyName).Errorf("cannot store supplier")
		}
	}

	return nil
}

func (g *Loader) loadData() (*LoaderData, error) {
	var categories []*Category
	err := g.loadCSV(path.Join(g.csvDirectory, "categories.csv"), &categories)
	if err != nil {
		return nil, fmt.Errorf("error parsing categories file %v", err)
	}

	var customers []*Customer
	err = g.loadCSV(path.Join(g.csvDirectory, "customers.csv"), &customers)
	if err != nil {
		return nil, fmt.Errorf("error parsing customers file %v", err)
	}

	var employees []*Employee
	err = g.loadCSV(path.Join(g.csvDirectory, "employees.csv"), &employees)
	if err != nil {
		return nil, fmt.Errorf("error parsing employees file %v", err)
	}

	var orderDetails []*OrderDetail
	err = g.loadCSV(path.Join(g.csvDirectory, "order_details.csv"), &orderDetails)
	if err != nil {
		return nil, fmt.Errorf("error parsing order_details file %v", err)
	}

	var orders []*Order
	err = g.loadCSV(path.Join(g.csvDirectory, "orders.csv"), &orders)
	if err != nil {
		return nil, fmt.Errorf("error parsing orders file %v", err)
	}

	var products []*Product
	err = g.loadCSV(path.Join(g.csvDirectory, "products.csv"), &products)
	if err != nil {
		return nil, fmt.Errorf("error parsing products file %v", err)
	}

	var shippers []*Shipper
	err = g.loadCSV(path.Join(g.csvDirectory, "shippers.csv"), &shippers)
	if err != nil {
		return nil, fmt.Errorf("error parsing shippers file %v", err)
	}

	var suppliers []*Supplier
	err = g.loadCSV(path.Join(g.csvDirectory, "suppliers.csv"), &suppliers)
	if err != nil {
		return nil, fmt.Errorf("error parsing suppliers file %v", err)
	}

	return &LoaderData{
		Categories:   categories,
		Customers:    customers,
		Employees:    employees,
		OrderDetails: orderDetails,
		Orders:       orders,
		Products:     products,
		Shippers:     shippers,
		Suppliers:    suppliers,
	}, nil
}

func (g *Loader) loadCSV(filename string, out interface{}) error {
	file, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not read file %v: %v", filename, err)
	}
	if file == nil {
		return fmt.Errorf("")
	}
	defer Close(file)

	err = gocsv.UnmarshalFile(file, out)
	if err != nil {
		return fmt.Errorf("could not parse CSV file %v: %v", filename, err)
	}

	return nil
}

func Close(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.WithError(err).Errorf("error closing file")
	}
}

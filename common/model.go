package common

type Category struct {
	CategoryID   int
	CategoryName string
	Description  string
	Picture      string
}

type Customer struct {
	CustomerID   string
	CompanyName  string
	ContactName  string
	ContactTitle string
	Address      string
	City         string
	Region       string
	PostalCode   string
	Country      string
	Phone        string
	Fax          string
}

type Employee struct {
	EmployeeID      int
	LastName        string
	FirstName       string
	Title           string
	TitleOfCourtesy string
	BirthDate       string
	HireDate        string
	Address         string
	City            string
	Region          string
	PostalCode      string
	Country         string
	HomePhone       string
	Extension       string
	Photo           string
	Notes           string
	ReportsTo       string
	PhotoPath       string
}

type OrderDetail struct {
	OrderID   int
	ProductID int
	UnitPrice string
	Quantity  string
	Discount  string
}

type Order struct {
	OrderID        int
	CustomerID     string
	EmployeeID     int
	OrderDate      string
	RequiredDate   string
	ShippedDate    string
	ShipVia        string
	Freight        string
	ShipName       string
	ShipAddress    string
	ShipCity       string
	ShipRegion     string
	ShipPostalCode string
	ShipCountry    string
}

type Product struct {
	ProductID       int
	ProductName     string
	SupplierID      int
	CategoryID      int
	QuantityPerUnit string
	UnitPrice       string
	UnitsInStock    string
	UnitsOnOrder    string
	ReorderLevel    string
	Discontinued    string
}

type Shipper struct {
	ShipperID   int
	CompanyName string
	Phone       string
}

type Supplier struct {
	SupplierID   int
	CompanyName  string
	ContactName  string
	ContactTitle string
	Address      string
	City         string
	Region       string
	PostalCode   string
	Country      string
	Phone        string
	Fax          string
	HomePage     string
}

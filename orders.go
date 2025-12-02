package main

type Order struct {
	OrderID    string       `json:"orderId"`
	CustomerID string       `json:"customerId"`
	Items      []Item       `json:"items"`
	Status     Status       `json:"status"`
	Shipping   ShippingInfo `json:"shipping"`
}

type ShippingInfo struct {
	Address1   string `json:"address1"`
	Address2   string `json:"address2"`
	City       string `json:"city"`
	Province   string `json:"province"`
	PostalCode string `json:"postalCode"`
	Country    string `json:"country"`
}

type Status int

const (
	Pending Status = iota
	Processing
	Complete
)

type Item struct {
	Product  int     `json:"productId"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type OrderRepo interface {
	GetPendingOrders() ([]Order, error)
	GetOrder(id string) (Order, error)
	InsertOrders(orders []Order) error
	UpdateOrder(order Order) error
}

type OrderService struct {
	repo OrderRepo
}

func NewOrderService(repo OrderRepo) *OrderService {
	return &OrderService{repo}
}

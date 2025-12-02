package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// Valid database API types
const (
	AZURE_COSMOS_DB_SQL_API = "cosmosdbsql"
)

func main() {
	var orderService *OrderService

	// Get the database API type
	apiType := os.Getenv("ORDER_DB_API")
	switch apiType {
	case "cosmosdbsql":
		log.Printf("Using Azure CosmosDB SQL API")
	default:
		log.Printf("Using MongoDB API")
	}

	// Initialize the database
	orderService, err := initDatabase(apiType)
	if err != nil {
		log.Printf("Failed to initialize database: %s", err)
		os.Exit(1)
	}

	// start background listener
	go startOrderListener(orderService)

	router := gin.Default()
	router.Use(cors.Default())
	router.Use(OrderMiddleware(orderService))
	router.GET("/order/fetch", fetchOrders)
	router.GET("/order/:id", getOrder)
	router.PUT("/order", updateOrder)
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"version": os.Getenv("APP_VERSION"),
		})
	})
	router.Run(":3001")
}

// startOrderListener sets up the client and calls the shared listener function
func startOrderListener(service *OrderService) {
	ctx := context.Background()
	orderQueueName := os.Getenv("ORDER_QUEUE_NAME")
	if orderQueueName == "" {
		orderQueueName = os.Getenv("ASB_QUEUE_NAME")
	}

	if orderQueueName == "" {
		log.Fatalf("CRITICAL: No queue name configured. Listener cannot start.")
	}

	var client *azservicebus.Client
	var err error

	// Auth: Connection String/SAS Key
	asbConnStr := os.Getenv("ASB_CONNECTION_STRING")
	if asbConnStr != "" {
		client, err = azservicebus.NewClientFromConnectionString(asbConnStr, nil)
		if err != nil {
			log.Fatalf("Failed to create ASB client: %v", err)
		}
		log.Println("Listener connected via Connection String.")
	} else {
		// Auth: Workload Identity
		hostName := os.Getenv("AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE")
		cred, _ := azidentity.NewDefaultAzureCredential(nil)
		client, err = azservicebus.NewClient(hostName, cred, nil)
		if err != nil {
			log.Fatalf("Failed to create ASB client: %v", err)
		}
		log.Println("Listener connected via Workload Identity.")
	}

	// Create shipping sender from environment variable
	shippingQueueName := os.Getenv("SHIPPING_QUEUE_NAME")
	if shippingQueueName == "" {
		shippingQueueName = "shipping"
	}

	shippingSender, err := client.NewSender(shippingQueueName, nil)
	if err != nil {
		log.Fatalf("Failed to create shipping sender: %v", err)
	}

	// Define the handler function for processing each order
	saveToDbHandler := func(order Order) error {
		log.Printf("Processing Order ID: %s", order.OrderID)

		// Insert into MongoDB (Existing)
		err := service.repo.InsertOrders([]Order{order})
		if err != nil {
			log.Printf("DB Error: %v", err)
			return err
		}

		// Create a specific payload for the shipping service
		shippingPayload := map[string]interface{}{
			"orderId":  order.OrderID,
			"shipping": order.Shipping,
			"status":   "ReadyForShipment",
		}

		body, err := json.Marshal(shippingPayload)
		if err != nil {
			log.Printf("Failed to marshal shipping payload: %v", err)
			// Don't fail the order just because shipping notification failed
			return nil
		}

		err = shippingSender.SendMessage(ctx, &azservicebus.Message{
			Body: body,
		}, nil)

		if err != nil {
			log.Printf("Failed to send to shipping queue: %v", err)
			// Ideally, you might want to implement a retry here or flag the DB record
		} else {
			log.Printf("Shipping request sent for Order %s", order.OrderID)
		}

		log.Println("Order processing complete.")
		return nil
	}

	// Call the new function from orderqueue.go
	err = ListenForOrdersASB(ctx, client, orderQueueName, saveToDbHandler)
	if err != nil {
		log.Fatalf("Listener stopped: %v", err)
	}
}

// Fetches orders from database that are pending
func fetchOrders(c *gin.Context) {
	client, ok := c.MustGet("orderService").(*OrderService)
	if !ok {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	orders, err := client.repo.GetPendingOrders()
	if err != nil {
		log.Printf("Failed to get pending orders: %s", err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.IndentedJSON(http.StatusOK, orders)
}

// OrderMiddleware is a middleware function that injects the order service into the request context
func OrderMiddleware(orderService *OrderService) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("orderService", orderService)
		c.Next()
	}
}

// Fetches orders from the order queue and stores them in database
// func fetchOrders(c *gin.Context) {
// 	client, ok := c.MustGet("orderService").(*OrderService)
// 	if !ok {
// 		log.Printf("Failed to get order service")
// 		c.AbortWithStatus(http.StatusInternalServerError)
// 		return
// 	}

// 	// Get orders from the queue
// 	orders, err := getOrdersFromQueue()
// 	if err != nil {
// 		log.Printf("Failed to fetch orders from queue: %s", err)
// 		c.AbortWithStatus(http.StatusInternalServerError)
// 		return
// 	}

// 	// Save orders to database
// 	err = client.repo.InsertOrders(orders)
// 	if err != nil {
// 		log.Printf("Failed to save orders to database: %s", err)
// 		c.AbortWithStatus(http.StatusInternalServerError)
// 		return
// 	}

// 	// Return the orders to be processed
// 	orders, err = client.repo.GetPendingOrders()
// 	if err != nil {
// 		log.Printf("Failed to get pending orders from database: %s", err)
// 		c.AbortWithStatus(http.StatusInternalServerError)
// 		return
// 	}

// 	c.IndentedJSON(http.StatusOK, orders)
// }

// Gets a single order from database by order ID
func getOrder(c *gin.Context) {
	client, ok := c.MustGet("orderService").(*OrderService)
	if !ok {
		log.Printf("Failed to get order service")
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		log.Printf("Failed to convert order id to int: %s", err)
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	sanitizedOrderId := strconv.FormatInt(int64(id), 10)

	order, err := client.repo.GetOrder(sanitizedOrderId)
	if err != nil {
		log.Printf("Failed to get order from database: %s", err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.IndentedJSON(http.StatusOK, order)
}

// Updates the status of an order
func updateOrder(c *gin.Context) {
	client, ok := c.MustGet("orderService").(*OrderService)
	if !ok {
		log.Printf("Failed to get order service")
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	// unmarsal the order from the request body
	var order Order
	if err := c.BindJSON(&order); err != nil {
		log.Printf("Failed to unmarshal order: %s", err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	id, err := strconv.Atoi(order.OrderID)
	if err != nil {
		log.Printf("Failed to convert order id to int: %s", err)
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	sanitizedOrderId := strconv.FormatInt(int64(id), 10)

	sanitizedOrder := Order{
		OrderID:    sanitizedOrderId,
		CustomerID: order.CustomerID,
		Items:      order.Items,
		Status:     order.Status,
	}

	err = client.repo.UpdateOrder(sanitizedOrder)
	if err != nil {
		log.Printf("Failed to update order status: %s", err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.SetAccepted("202")
}

// Gets an environment variable or exits if it is not set
func getEnvVar(varName string, fallbackVarNames ...string) string {
	value := os.Getenv(varName)
	if value == "" {
		for _, fallbackVarName := range fallbackVarNames {
			value = os.Getenv(fallbackVarName)
			if value == "" {
				break
			}
		}
		if value == "" {
			log.Printf("%s is not set", varName)
			if len(fallbackVarNames) > 0 {
				log.Printf("Tried fallback variables: %v", fallbackVarNames)
			}
			os.Exit(1)
		}
	}
	return value
}

// Initializes the database based on the API type
func initDatabase(apiType string) (*OrderService, error) {
	dbURI := getEnvVar("AZURE_COSMOS_RESOURCEENDPOINT", "ORDER_DB_URI")
	dbName := getEnvVar("ORDER_DB_NAME")

	switch apiType {
	case AZURE_COSMOS_DB_SQL_API:
		containerName := getEnvVar("ORDER_DB_CONTAINER_NAME")
		dbPartitionKey := getEnvVar("ORDER_DB_PARTITION_KEY")
		dbPartitionValue := getEnvVar("ORDER_DB_PARTITION_VALUE")

		// check if USE_WORKLOAD_IDENTITY_AUTH is set
		useWorkloadIdentityAuth := os.Getenv("USE_WORKLOAD_IDENTITY_AUTH")
		if useWorkloadIdentityAuth == "" {
			useWorkloadIdentityAuth = "false"
		}

		if useWorkloadIdentityAuth == "true" {
			cosmosRepo, err := NewCosmosDBOrderRepoWithManagedIdentity(dbURI, dbName, containerName, PartitionKey{dbPartitionKey, dbPartitionValue})
			if err != nil {
				return nil, err
			}
			return NewOrderService(cosmosRepo), nil
		} else {
			dbPassword := os.Getenv("ORDER_DB_PASSWORD")
			cosmosRepo, err := NewCosmosDBOrderRepo(dbURI, dbName, containerName, dbPassword, PartitionKey{dbPartitionKey, dbPartitionValue})
			if err != nil {
				return nil, err
			}
			return NewOrderService(cosmosRepo), nil
		}
	default:
		collectionName := getEnvVar("ORDER_DB_COLLECTION_NAME")
		dbUsername := os.Getenv("ORDER_DB_USERNAME")
		dbPassword := os.Getenv("ORDER_DB_PASSWORD")
		mongoRepo, err := NewMongoDBOrderRepo(dbURI, dbName, collectionName, dbUsername, dbPassword)
		if err != nil {
			return nil, err
		}
		return NewOrderService(mongoRepo), nil
	}
}

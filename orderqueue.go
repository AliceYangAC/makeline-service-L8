package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
)

// listenForOrdersASB keeps a persistent connection and processes messages as they arrive.
func ListenForOrdersASB(ctx context.Context, client *azservicebus.Client, orderQueueName string, handleOrder func(Order) error) error {
	receiver, err := client.NewReceiverForQueue(orderQueueName, nil)
	if err != nil {
		return fmt.Errorf("failed to create receiver: %w", err)
	}
	defer receiver.Close(ctx)

	log.Printf("Listening for orders on queue: %s...", orderQueueName)

	for {
		// ReceiveMessages blocks until a message arrives or context is cancelled
		messages, err := receiver.ReceiveMessages(ctx, 1, nil)

		if err != nil {
			// If context is canceled (shutdown), return gracefully
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			log.Printf("Error receiving message: %v", err)
			// Optional: Backoff/sleep here to prevent log spam on network failure
			time.Sleep(2 * time.Second)
			continue
		}

		for _, message := range messages {
			log.Printf("ASB message received: %s", string(message.Body))

			var order Order
			err := json.Unmarshal(message.Body, &order)
			if err != nil {
				var jsonStr string
				if errString := json.Unmarshal(message.Body, &jsonStr); errString == nil {
					err = json.Unmarshal([]byte(jsonStr), &order)
				}
			}

			if err != nil {
				log.Printf("Failed to unmarshal order: %v. Abandoning.", err)
				receiver.AbandonMessage(ctx, message, nil)
				continue
			}

			// 2. Validate/Fix Order Data
			if order.OrderID == "" {
				order.OrderID = strconv.Itoa(rand.Intn(100000))
			}

			order.Status = 1 // Pending

			// 3. Pass to Business Logic (The Handler)
			if err := handleOrder(order); err != nil {
				log.Printf("Handler failed: %v. Abandoning.", err)
				receiver.AbandonMessage(ctx, message, nil)
				continue
			}

			// 4. Complete
			if err := receiver.CompleteMessage(ctx, message, nil); err != nil {
				log.Printf("Failed to complete message: %v", err)
			}
		}
	}
}

// getOrdersFromQueueASB handles the Azure Service Bus message receiving logic.
func getOrdersFromQueueASB(ctx context.Context, client *azservicebus.Client, orderQueueName string) ([]Order, error) {
	var orders []Order

	receiver, err := client.NewReceiverForQueue(orderQueueName, nil)
	if err != nil {
		log.Fatalf("failed to create ASB receiver: %v", err)
	}
	defer receiver.Close(context.TODO())

	messages, err := receiver.ReceiveMessages(context.TODO(), 10, nil)
	if err != nil {
		log.Fatalf("failed to receive ASB messages: %v", err)
	}

	for _, message := range messages {
		log.Printf("ASB message received: %s\n", string(message.Body))

		var order Order
		err := json.Unmarshal(message.Body, &order)

		// If that fails, might be a double-encoded JSON String
		if err != nil {
			var jsonStr string
			// Try unwrapping it as a string first
			if errString := json.Unmarshal(message.Body, &jsonStr); errString == nil {
				// If that worked, unmarshal the inner string into the Order struct
				err = json.Unmarshal([]byte(jsonStr), &order)
			}
		}

		// If it STILL fails, the data is bad. Abandon the message.
		if err != nil {
			log.Printf("failed to unmarshal ASB message: %v", err)
			receiver.AbandonMessage(context.TODO(), message, nil)
			continue
		}

		// Generate ID if missing
		if order.OrderID == "" {
			order.OrderID = strconv.Itoa(rand.Intn(100000))
		}
		order.Status = 1 // Pending

		// Add to list
		orders = append(orders, order)

		err = receiver.CompleteMessage(context.TODO(), message, nil)
		if err != nil {
			log.Fatalf("failed to complete ASB message: %v", err)
		}
	}

	return orders, nil
}

// getOrdersFromQueueAMQP contains the original RabbitMQ connection and receiving logic.
func getOrdersFromQueueAMQP(ctx context.Context, orderQueueName string) ([]Order, error) {
	var orders []Order
	orderQueueUri := os.Getenv("ORDER_QUEUE_URI")
	orderQueueUsername := os.Getenv("ORDER_QUEUE_USERNAME")
	orderQueuePassword := os.Getenv("ORDER_QUEUE_PASSWORD")

	// NOTE: Simplified check based on existing logic.
	if orderQueueUri == "" || orderQueueUsername == "" || orderQueuePassword == "" {
		log.Printf("ERROR: Missing RabbitMQ credentials.")
		return nil, errors.New("missing RabbitMQ credentials")
	}

	conn, err := amqp.Dial(ctx, orderQueueUri, &amqp.ConnOptions{
		SASLType: amqp.SASLTypePlain(orderQueueUsername, orderQueuePassword),
	})
	if err != nil {
		log.Printf("failed to connect to order queue: %s", err)
		return nil, err
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Printf("unable to create a new session")
	}

	{
		receiver, err := session.NewReceiver(ctx, orderQueueName, nil)
		if err != nil {
			log.Printf("creating receiver link: %s", err)
			return nil, err
		}
		defer func() {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			receiver.Close(ctx)
			cancel()
		}()

		for {
			log.Printf("getting orders")

			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			msg, err := receiver.Receive(ctx, nil)
			if err != nil {
				if err.Error() == "context deadline exceeded" {
					log.Printf("no more orders for you: %v", err.Error())
					break
				} else {
					return nil, err
				}
			}

			messageBody := string(msg.GetData())
			log.Printf("message received: %s\n", messageBody)

			// Original code's double unmarshal logic is preserved for fidelity
			var jsonStr string
			err = json.Unmarshal(msg.GetData(), &jsonStr)
			if err != nil {
				log.Printf("failed to deserialize message: %s", err)
				return nil, err
			}
			order, err := unmarshalOrderFromQueue([]byte(jsonStr))
			// End Original double unmarshal logic

			if err != nil {
				log.Printf("failed to unmarshal message: %s", err)
				return nil, err
			}

			orders = append(orders, order)

			if err = receiver.AcceptMessage(context.TODO(), msg); err != nil {
				log.Printf("failure accepting message: %s", err)
				orders = orders[:len(orders)-1]
			}
		}
	}
	return orders, nil
}

func getOrdersFromQueue() ([]Order, error) {
	ctx := context.Background()

	// var orders []Order
	orderQueueName := os.Getenv("ORDER_QUEUE_NAME")

	if orderQueueName == "" {
		log.Printf("ORDER_QUEUE_NAME is not set")
		return nil, errors.New("ORDER_QUEUE_NAME is not set")
	}

	// Check for ASB_CONNECTION_STRING (SAS Key Auth for Local Dev)
	asbConnStr := os.Getenv("ASB_CONNECTION_STRING")
	if asbConnStr != "" {
		client, err := azservicebus.NewClientFromConnectionString(asbConnStr, nil)
		if err != nil {
			log.Fatalf("failed to create Service Bus client from connection string: %v", err)
		}
		fmt.Println("successfully created a service bus client using Connection String (ASB)")
		return getOrdersFromQueueASB(ctx, client, orderQueueName)
	}

	// Check for Workload Identity (Azure Deployment)
	useWorkloadIdentityAuth := os.Getenv("USE_WORKLOAD_IDENTITY_AUTH")
	if useWorkloadIdentityAuth == "true" {
		orderQueueHostName := os.Getenv("AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE")

		if orderQueueHostName == "" {
			log.Fatalf("Workload Identity requested, but ASB Namespace is not set.")
		}

		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			log.Fatalf("failed to obtain a workload identity credential: %v", err)
		}

		client, err := azservicebus.NewClient(orderQueueHostName, cred, nil)
		if err != nil {
			log.Fatalf("failed to create service bus client with workload identity: %v", err)
		}
		fmt.Println("successfully created a service bus client with workload identity credentials")
		return getOrdersFromQueueASB(ctx, client, orderQueueName)
	}

	// Fallback to Legacy AMQP (RabbitMQ)
	log.Printf("No ASB Configuration found. Falling back to RabbitMQ/Generic AMQP connection...")
	return getOrdersFromQueueAMQP(ctx, orderQueueName)
}

func unmarshalOrderFromQueue(data []byte) (Order, error) {
	var order Order

	err := json.Unmarshal(data, &order)
	if err != nil {
		log.Printf("failed to unmarshal order: %v\n", err)
		return Order{}, err
	}

	// add orderkey to order
	order.OrderID = strconv.Itoa(rand.Intn(100000))

	// set the status to pending
	order.Status = Pending

	return order, nil
}

package main

import (
	"context"
	"crypto/tls"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBOrderRepo struct {
	db *mongo.Collection
}

func NewMongoDBOrderRepo(mongoUri string, mongoDb string, mongoCollection string, mongoUser string, mongoPassword string) (*MongoDBOrderRepo, error) {
	// create a context
	ctx := context.Background()

	// create a mongo client
	var clientOptions *options.ClientOptions
	if mongoUser == "" && mongoPassword == "" {
		clientOptions = options.Client().ApplyURI(mongoUri)
	} else {
		clientOptions = options.Client().ApplyURI(mongoUri).
			SetAuth(options.Credential{
				AuthSource: mongoDb,
				Username:   mongoUser,
				Password:   mongoPassword,
			}).
			SetTLSConfig(&tls.Config{InsecureSkipVerify: false})
	}

	mongoClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Printf("failed to connect to mongodb: %s", err)
		return nil, err
	}

	err = mongoClient.Ping(ctx, nil)
	if err != nil {
		log.Printf("failed to ping database: %s", err)
		return nil, err
	} else {
		log.Printf("pong from database")
	}

	// get a handle for the collection
	collection := mongoClient.Database(mongoDb).Collection(mongoCollection)
	//defer collection.Database().Client().Disconnect(context.Background())

	return &MongoDBOrderRepo{collection}, nil
}

func (r *MongoDBOrderRepo) GetAllOrders() ([]Order, error) {
	ctx := context.TODO()

	var orders []Order

	cursor, err := r.db.Find(ctx, bson.M{})

	if err != nil {
		log.Printf("Failed to find records: %s", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	// Check if there was an error during iteration
	if err := cursor.Err(); err != nil {
		log.Printf("Failed to find records: %s", err)
		return nil, err
	}

	// Iterate over the cursor and decode each document
	for cursor.Next(ctx) {
		var order Order
		// Ensure your struct BSON tags match your DB if decoding fails
		if err := cursor.Decode(&order); err != nil {
			log.Printf("Failed to decode order: %s", err)
			return nil, err
		}
		orders = append(orders, order)
	}

	return orders, nil
}

// func (r *MongoDBOrderRepo) GetPendingOrders() ([]Order, error) {
// 	ctx := context.TODO()

// 	var orders []Order
// 	cursor, err := r.db.Find(ctx, bson.M{"status": Pending})
// 	if err != nil {
// 		log.Printf("Failed to find records: %s", err)
// 		return nil, err
// 	}
// 	defer cursor.Close(ctx)

// 	// Check if there was an error during iteration
// 	if err := cursor.Err(); err != nil {
// 		log.Printf("Failed to find records: %s", err)
// 		return nil, err
// 	}

// 	// Iterate over the cursor and decode each document
// 	for cursor.Next(ctx) {
// 		var pendingOrder Order
// 		if err := cursor.Decode(&pendingOrder); err != nil {
// 			log.Printf("Failed to decode order: %s", err)
// 			return nil, err
// 		}
// 		orders = append(orders, pendingOrder)
// 	}

// 	return orders, nil
// }

func (r *MongoDBOrderRepo) GetOrder(id string) (Order, error) {
	var ctx = context.TODO()

	filter := bson.D{{Key: "orderid", Value: bson.D{{Key: "$eq", Value: id}}}}

	singleResult := r.db.FindOne(ctx, filter)

	var order Order
	err := singleResult.Decode(&order)
	if err != nil {
		log.Printf("Failed to decode order: %s", err)
		return order, err
	}

	return order, nil
}

func (r *MongoDBOrderRepo) InsertOrders(orders []Order) error {
	ctx := context.TODO()

	var ordersInterface []interface{}
	for _, o := range orders {
		ordersInterface = append(ordersInterface, interface{}(o))
	}

	if len(ordersInterface) == 0 {
		log.Printf("No orders to insert into database")
	} else {
		// Insert orders
		insertResult, err := r.db.InsertMany(ctx, ordersInterface)
		if err != nil {
			log.Printf("Failed to insert order: %s", err)
			return err
		}

		log.Printf("Inserted %v documents into database\n", len(insertResult.InsertedIDs))
	}
	return nil
}

func (r *MongoDBOrderRepo) UpdateOrder(order Order) error {
	var ctx = context.TODO()

	filter := bson.D{{Key: "orderid", Value: order.OrderID}}

	// Define the update: Set Status AND Items
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: order.Status},
			{Key: "items", Value: order.Items},
		}},
	}

	log.Printf("Updating order %s: Status=%v, ItemsCount=%d", order.OrderID, order.Status, len(order.Items))

	updateResult, err := r.db.UpdateOne(ctx, filter, update)
	if err != nil {
		log.Printf("Failed to update order: %s", err)
		return err
	}

	log.Printf("Matched %v documents and updated %v documents.\n", updateResult.MatchedCount, updateResult.ModifiedCount)
	return nil
}

// Deletes an order by ID (Cancel Order)
func (r *MongoDBOrderRepo) DeleteOrder(id string) error {
	ctx := context.TODO()

	filter := bson.D{{Key: "orderid", Value: id}}

	deleteResult, err := r.db.DeleteOne(ctx, filter)
	if err != nil {
		log.Printf("Failed to delete order: %s", err)
		return err
	}

	if deleteResult.DeletedCount == 0 {
		log.Printf("No order found with ID %s to delete", id)
	} else {
		log.Printf("Deleted order with ID %s", id)
	}

	return nil
}

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	/////////////MONGODB__CONNECTION__STARTS /////////////////////
	clientOptions := options.Client().ApplyURI(
		"mongodb://admin:secret@localhost:27017",
	).SetDirect(true)

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Disconnect(context.Background())

	databaseName := "shamim"
	collectionName := "col1"
	collection := client.Database(databaseName).Collection(collectionName)

	/////////////MONGODB__CONNECTION__ENDS /////////////////////

	/////////////POSTGRES__CONNECTION__STARTS /////////////////////
	pgConnectionString := "user=root password=secret dbname=postgres sslmode=disable"
	pgDB, err := sql.Open("postgres", pgConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer pgDB.Close()
	/////////////POSTGRES__CONNECTION__ENDS /////////////////////

	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"operationType", bson.D{{"$in", bson.A{"insert", "update", "replace", "delete"}}}}}}},
	}
	changeStreamOptions := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	var resumeToken bson.Raw
	var okk bool

	for {
		if okk {
			changeStreamOptions = changeStreamOptions.SetResumeAfter(resumeToken)
		}

		changeStream, err := collection.Watch(context.Background(), pipeline, changeStreamOptions)
		if err != nil {
			log.Println("Error creating change stream:", err)
			continue // Continue to the next iteration to retry the change stream
		}

		fmt.Println("Listening for changes...")

		for changeStream.Next(context.Background()) {
			resumeToken = changeStream.ResumeToken()

			var changeEvent bson.M
			if err := changeStream.Decode(&changeEvent); err != nil {
				log.Println("Error decoding change event:", err)
				continue // Continue to the next iteration to handle the next change event
			}

			operationType, ok := changeEvent["operationType"].(string)
			if !ok {
				log.Fatal("Error extracting 'operationType' from change event")
			}

			if operationType == "insert" || operationType == "update" || operationType == "replace" {
				id := changeEvent["fullDocument"].(primitive.M)["_id"].(primitive.ObjectID).Hex()
				item := changeEvent["fullDocument"].(primitive.M)["item"].(string)
				qtyInt32, ok := changeEvent["fullDocument"].(primitive.M)["qty"].(int32)
				if !ok {
					log.Println("Error extracting 'qty' as int32 from BSON")
					continue // Continue to the next iteration to handle the next change event
				}
				qty := int(qtyInt32)
				sizeJSON, err := json.Marshal(changeEvent["fullDocument"].(primitive.M)["size"])
				if err != nil {
					log.Println("Error converting 'size' to JSON:", err)
					continue // Continue to the next iteration to handle the next change event
				}
				tagsJSON, err := json.Marshal(changeEvent["fullDocument"].(primitive.M)["tags"])
				if err != nil {
					log.Println("Error converting 'tags' to JSON:", err)
					continue // Continue to the next iteration to handle the next change event
				}

				stmt, err := pgDB.Prepare(`
					INSERT INTO not_json_table_2 (id, item, qty, size, tags)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (id) DO UPDATE
					SET item = excluded.item,
						qty = excluded.qty,
						size = excluded.size,
						tags = excluded.tags
				`)
				if err != nil {
					log.Println("Error preparing SQL statement:", err)
					continue // Continue to the next iteration to handle the next change event
				}
				defer stmt.Close()

				_, err = stmt.Exec(id, item, qty, sizeJSON, tagsJSON)
				if err != nil {
					log.Println("Error executing SQL statement:", err)
					continue // Continue to the next iteration to handle the next change event
				}

				fmt.Println("Data inserted or updated in PostgreSQL")
			} else if operationType == "delete" {
				idToDelete := changeEvent["documentKey"].(primitive.M)["_id"].(primitive.ObjectID).Hex()

				stmt, err := pgDB.Prepare("DELETE FROM not_json_table_2 WHERE id = $1")
				if err != nil {
					log.Println("Error preparing SQL delete statement:", err)
					continue // Continue to the next iteration to handle the next change event
				}
				defer stmt.Close()

				_, err = stmt.Exec(idToDelete)
				if err != nil {
					log.Println("Error executing SQL delete statement:", err)
					continue // Continue to the next iteration to handle the next change event
				}

				fmt.Println("Data deleted from PostgreSQL")
			}

			fmt.Println("Change detected..")
			fmt.Println(changeEvent)

			if !okk {
				/// close the connection to mongodb
				time.Sleep(30 * time.Second)
				okk = true
			}

		}

		if err := changeStream.Err(); err != nil {
			log.Println("Error in change stream:", err)
		}

		// if !okk {
		// 	// create connection to mongodb again
		// }
		// // Set okk to true after the first successful iteration to include the resume token.
		// okk = true
	}
}

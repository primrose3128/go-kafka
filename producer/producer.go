package main

import(
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct{
	Text string `form:"text json:"text"`
}

func main(){
	//fiber uses fasthttp, will try gin instead
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	app.Listen(":3000")
}

func ConnectProducer(brokersUrl []string)(sarama.SyncProducer, error){
	//setting config
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true;
	//RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements it must see before responding.
	//The RequiredAcks parameter is used to control the acknowledgement mechanism when the producer sends messages. The default value is WaitForLocal, which means that once the message is sent to the Leader Broker and the Leader confirms the message has been written, it is returned immediately.
	config.Producer.RequiredAcks = sarama.WaitForAll //when the message is sent it will wait for leader broker and relevant followers to confirm the message has been written and and return it
	config.Producer.Retry.Max = 5 //no of tries

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil{
		return nil, err
	}
	return conn, nil
}

func PustCommentToQueue(topic string, message []byte) error{
	brokersUrl := [] string{"localhost: 29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil{
		return err
	}

	//defer statements delay the execution of the function or method or an anonymous method until the nearby functions returns. 
	defer producer.Close() //will close the connection to the producer at the end of the function
	
	//This is the message that will be sent that is to be stored
	msg := &sarama.ProducerMessage{
		Topic : topic,
		Value : sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)

	fmt.Println("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

//here fiber.Ctx is fiber context where request body is present
func createComment(c *fiber.Ctx) error{
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil{
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success" : false,
			"message" : err,
		})
		return err;
	}

	cmtInBytes, err := json.Marshal(cmt); //convert the struct into byte
	PustCommentToQueue("comments", cmtInBytes);

	err = c.JSON(&fiber.Map{
		"success": true,
		"message" : "Comment pushed successfully",
		"comment" : cmt,
	})

	if err != nil{
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message" : "Error creating product",
		})
		return err;
	}

	return err;
}
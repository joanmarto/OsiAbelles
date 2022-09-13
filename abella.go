package main

//NOM: Joan Martorell Ferriol
//LINK: https://youtu.be/G-HzaKKKCoU
import (
	"bytes"
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	MAX_PORCIONS = 10
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	nom := os.Args[len(os.Args)-1]
	log.Printf(" [*] Hola, soc l'abella %s", nom)

	//Coa per consumir permisos per prodir mel
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"permisos", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	//Coa per despertar l'os
	conn_desp, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn_desp.Close()

	ch_desp, err := conn_desp.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch_desp.Close()

	d, err := ch_desp.QueueDeclare(
		"desperta", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//Coa per rebre el misatje de l'os que indica que han d'acabar les abelles
	conn_end, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn_end.Close()

	ch_end, err := conn_end.Channel()
	failOnError(err, "Failed to open a channel")
	err = ch_end.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	defer ch_end.Close()

	end, err := ch_end.QueueDeclare(
		"final", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")
	ch_end.QueueBind(
		"final",  // queue name
		end.Name, // routing key
		"logs",   // exchange
		false,
		nil,
	)

	msg_end, err := ch_end.Consume(
		end.Name, // queue
		"",       // consumer
		true,     // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	failOnError(err, "Failed to register a consumer")

	continua := true

	for continua {
		for i := range msgs {
			log.Printf("L'abella %s produeix mel %s", nom, i.Body)
			time.Sleep(time.Second)
			//si i.Body == 10 el pot està ple i hem de despertar l'os
			comp := bytes.Compare(i.Body, []byte(strconv.Itoa(MAX_PORCIONS)))
			if comp == 0 {
				log.Printf("L'abella %s desperta l'os", nom)
				err = ch_desp.Publish(
					"",     // exchange
					d.Name, // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(nom), //Nom de l'abella que desperta l'os
					})
				failOnError(err, "Failed to publish a message")
			}
		}

		forever := make(chan bool)
		go func() {
			<-msg_end
			continua = false
			forever <- true
		}()
		<-forever
	}
	log.Print("El pot està romput, no es pot omplir de mel!!!")
	log.Printf("L'abella %s se'n va")
	log.Print("SIMULACIÓ ACABADA")

	//Tancam les coes
	defer ch.Close()
	defer ch_desp.Close()
	defer ch_end.Close()
}

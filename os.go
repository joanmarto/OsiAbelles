package main

//NOM : Joan Martorell Ferriol
//LINK: https://youtu.be/G-HzaKKKCoU
import (
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	MIDA_POT = 10 //Nombre de porcions de mel que hi ha al pot
	MAX_POTS = 3  //Nombre de vegades que l'os menja
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {

	//Coa per produir permisos de producció de mel
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

	//Coa per obtenir el misatje de l'abella que desperta l'os
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

	msg, err := ch_desp.Consume(
		d.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	//Coa per avisar a les abelles que han d'acabar
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

	//Inici del programa
	for voltes := 1; voltes <= MAX_POTS; voltes++ {
		for i := 1; i <= MIDA_POT; i++ {
			//Cream els permisos de producció de mel i els posam a la coa
			err = ch.Publish(
				"",     // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(strconv.Itoa(i)),
				})
			failOnError(err, "Failed to publish a message")
		}

		//L'os menja el pot i se'n va a dormir fins que l'abella el desperta
		log.Print("L'os se'n va a dormir")
		time.Sleep(3 * time.Second)
		//Esperam a que l'abella desperti l'os
		desp := <-msg
		log.Printf("L'abella %s desperta l'os. L'os menja %d/3", desp.Body, voltes)
	}
	log.Printf("L'os està ple, ha romput el pot!!!")
	err = ch_end.Publish(
		"logs",   // exchange
		end.Name, // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("fi"),
		})
	failOnError(err, "Failed to publish a message")

	//Tancam les coes
	log.Print("Eliminant cues...")
	defer ch.Close()
	defer ch_desp.Close()
	defer ch_end.Close()
	log.Print("SIMULACIÓ ACABADA")
}

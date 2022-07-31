package main

import (
	mailbox "github.com/hamidOyeyiola/kafka-mail-service/mail_service"
	kafteria "github.com/hamidOyeyiola/kafka-mail-service/service"
)

func main() {
	m := mailbox.MailServer{}
	chef := kafteria.NewChef(&m, &mailbox.MailService_ServiceDesc)
	chef.Run()
}

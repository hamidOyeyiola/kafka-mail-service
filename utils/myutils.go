package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/mail"
	"time"
)

type EmailAddress string

type Date struct {
	year  int
	month time.Month
	day   int
}

func init() {
	rand.Seed(time.Now().Unix())
}

func (d *Date) String() string {
	return fmt.Sprintf("%s, %d %d", d.month, d.day, d.year)
}

func NewDate() *Date {
	d := Date{}
	d.year, d.month, d.day = time.Now().Date()
	return &d
}

func EncryptPassword(password string) string {
	data := []byte(password)
	return fmt.Sprintf("%x", md5.Sum(data))
}

func Hash(value string) string {
	data := []byte(value)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func (e EmailAddress) IsValid() bool {
	_, err := mail.ParseAddress(string(e))
	return err == nil
}

func GetSessionToken() string {
	return fmt.Sprintf("%d%d%d", rand.Int(), rand.Int(), time.Now().Nanosecond())
}

func MakeDeadline(d time.Duration) string {
	deadline := time.Now().Add(d)
	b, _ := json.Marshal(deadline)
	return string(b)
}

func IsNotDeadline(deadline string) bool {
	u := time.Time{}
	json.Unmarshal([]byte(deadline), &u)
	return time.Now().Before(u)
}

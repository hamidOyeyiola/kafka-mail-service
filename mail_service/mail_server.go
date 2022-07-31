package mailbox

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/hamidOyeyiola/kafka-mail-service/utils"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/net/context"
)

type mailbox struct {
	db       *sql.DB
	mu       sync.Mutex
	conns    int
	Database string
}

var mailboxInstance *mailbox

func init() {
	mailboxInstance = new(mailbox)
	mailboxInstance.Database = "hamid:@tcp(localhost:3306)/mailbox"
}

func (m *mailbox) Open() bool {
	m.mu.Lock()
	if m.db == nil {
		db, err := sql.Open("mysql", m.Database)
		if err != nil {
			return false
		}
		db.SetConnMaxLifetime(3 * time.Minute)
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(10)
		m.db = db
	}
	m.conns++
	m.mu.Unlock()
	return true
}

func (m *mailbox) Close() bool {
	m.mu.Lock()
	m.conns--
	if m.conns == 0 {
		err := m.db.Close()
		if err != nil {
			return false
		}
		m.db = nil
	}
	m.mu.Unlock()
	return true
}

type MailServer struct {
	UnimplementedMailServiceServer
}

func (s *MailServer) Register(ctx context.Context, user *UserID) (res *Response, err error) {
	res = new(Response)
	mailboxInstance.register(user, res)
	return res, err
}

func (s *MailServer) PostRequest(ctx context.Context, req *Request) (res *Response, err error) {

	res = new(Response)
	switch req.Routine {
	case "send":
		err = mailboxInstance.send(req, res)
	case "draft":
		err = mailboxInstance.draft(req, res)
	case "save":
		err = mailboxInstance.save(req, res)
	case "inbox":
		err = mailboxInstance.inbox(req, res)
	case "outbox":
		err = mailboxInstance.outbox(req, res)
	case "delete":
		err = mailboxInstance.delete(req, res)
	case "read":
		err = mailboxInstance.read(req, res)
	}
	return res, err
}

func (m *mailbox) send(req *Request, res *Response) error {
	m.Open()
	defer m.Close()

	msgID := req.Email.GetId()
	if msgID == "" {
		n, err := m.saveHelper(req, res)
		if err != nil {
			d := new(Mail)
			d.Subject = "Could not Create a new Message."
			res.Mails = append(res.Mails, d)
			return err
		}
		msgID = fmt.Sprintf("%d", n)
	}
	query := fmt.Sprintf("SELECT outbox FROM users WHERE userid = '%s'", req.Email.GetSender())
	rows, err := m.db.Query(query)
	if err != nil {
		d := new(Mail)
		d.Subject = "Invalid Source Address."
		res.Mails = append(res.Mails, d)
		return err
	}
	f1 := ""
	rows.Next()
	err = rows.Scan(&f1)
	if f1 == "" {
		f1 = fmt.Sprintf("%s", msgID)
	} else {
		f1 = f1 + fmt.Sprintf(",%s", msgID)
	}
	query = fmt.Sprintf("SELECT inbox FROM users WHERE userid = '%s'", req.Email.GetRecipient())
	rows, err = m.db.Query(query)
	if err != nil {
		d := new(Mail)
		d.Subject = "Invalid Destination Address."
		res.Mails = append(res.Mails, d)
		return err
	}
	f2 := ""
	rows.Next()
	err = rows.Scan(&f2)
	if f2 == "" {
		f2 = fmt.Sprintf("%s", msgID)
	} else {
		f2 = f2 + fmt.Sprintf(",%s", msgID)
	}

	query = "UPDATE users SET inbox=? WHERE userid=?"
	stmt, err := m.db.Prepare(query)
	_, err = stmt.Exec(f2, req.Email.GetRecipient())

	query = "UPDATE users SET outbox=? WHERE userid=?"
	stmt, err = m.db.Prepare(query)
	_, err = stmt.Exec(f1, req.Email.GetSender())

	//Remove message record from draft

	query = "UPDATE messages SET sentAt=? WHERE id=?"
	stmt, err = m.db.Prepare(query)
	_, err = stmt.Exec(time.Now(), msgID)
	query = fmt.Sprintf("SELECT * FROM messages WHERE id IN (%s)", f1)
	rows, err = m.db.Query(query)
	for rows.Next() {
		d := new(Mail)
		id := 0
		var createdAt, sentAt string
		err = rows.Scan(&d.Sender, &d.Recipient, &d.Body, &d.Subject, &id, &createdAt, &sentAt)
		d.Id = fmt.Sprintf("%d", id)
		res.Mails = append(res.Mails, d)
	}
	return err
}

func (m *mailbox) saveHelper(req *Request, res *Response) (int64, error) {
	query := queryToInsertMail(req.Email)
	result, err := m.db.Exec(query)
	if err != nil {
		d := new(Mail)
		d.Subject = "Could not Create a new Message."
		res.Mails = append(res.Mails, d)
		return 0, err
	}
	return result.LastInsertId()
}

func (m *mailbox) save(req *Request, res *Response) error {
	m.Open()
	defer m.Close()
	n, err := m.saveHelper(req, res)
	query := fmt.Sprintf("SELECT draft FROM users WHERE userid = '%s'", req.User.GetId())
	rows, err := m.db.Query(query)
	f := ""
	rows.Next()
	err = rows.Scan(&f)
	if f == "" {
		f = fmt.Sprintf("%d", n)
	} else {
		f = f + fmt.Sprintf(",%d", n)
	}
	query = "UPDATE users SET draft=? WHERE userid=?"
	stmt, err := m.db.Prepare(query)
	_, err = stmt.Exec(f, req.User.GetId())
	query = fmt.Sprintf("SELECT * FROM messages WHERE id IN (%s)", f)
	rows, err = m.db.Query(query)
	for rows.Next() {
		d := new(Mail)
		id := 0
		var createdAt, sentAt string
		err = rows.Scan(&d.Sender, &d.Recipient, &d.Body, &d.Subject, &id, &createdAt, &sentAt)
		d.Id = fmt.Sprintf("%d", id)
		res.Mails = append(res.Mails, d)
	}
	return err
}

func (m *mailbox) readAll(from string, req *Request, res *Response) error {
	query := fmt.Sprintf("SELECT %s FROM users WHERE userid = '%s'", from, req.User.GetId())
	rows, err := m.db.Query(query)
	f := ""
	rows.Next()
	err = rows.Scan(&f)
	query = fmt.Sprintf("SELECT * FROM messages WHERE id IN (%s)", f)
	rows, err = m.db.Query(query)
	for rows.Next() {
		d := new(Mail)
		id := 0
		var createdAt, sentAt string
		err = rows.Scan(&d.Sender, &d.Recipient, &d.Body, &d.Subject, &id, &createdAt, &sentAt)
		d.Id = fmt.Sprintf("%d", id)
		res.Mails = append(res.Mails, d)
	}
	return err
}

func (m *mailbox) inbox(req *Request, res *Response) error {
	m.Open()
	defer m.Close()
	return m.readAll("inbox", req, res)
}

func (m *mailbox) draft(req *Request, res *Response) error {
	m.Open()
	defer m.Close()
	return m.readAll("draft", req, res)
}

func (m *mailbox) outbox(req *Request, res *Response) error {
	m.Open()
	defer m.Close()
	return m.readAll("outbox", req, res)
}

func (m *mailbox) delete(req *Request, res *Response) error {
	m.Open()
	defer m.Close()
	query := fmt.Sprintf("DELETE FROM messages WHERE id IN (%s)", req.Email.GetId())
	_, err := m.db.Query(query)
	d := new(Mail)
	if err != nil {
		d.Subject = "Failed to delete a message."
		res.Mails = append(res.Mails, d)
	} else {
		d.Subject = "Message successfully deleted."
		res.Mails = append(res.Mails, d)
	}
	return err
}

func (m *mailbox) read(req *Request, res *Response) error {
	m.Open()
	defer m.Close()
	query := fmt.Sprintf("SELECT * FROM messages WHERE id IN (%s)", req.Email.GetId())
	rows, err := m.db.Query(query)
	if err != nil {
		d := new(Mail)
		d.Subject = "Could not read Message."
		res.Mails = append(res.Mails, d)
		return err
	}
	for rows.Next() {
		d := new(Mail)
		id := 0
		var createdAt, sentAt string
		err = rows.Scan(&d.Sender, &d.Recipient, &d.Body, &d.Subject, &id, &createdAt, &sentAt)
		d.Id = fmt.Sprintf("%d", id)
		res.Mails = append(res.Mails, d)
	}
	return err
}

func (m *mailbox) register(user *UserID, res *Response) error {
	m.Open()
	defer m.Close()

	d := new(Mail)
	u := User{
		ID:           user.Id,
		RegisteredOn: utils.NewDate().String(),
	}

	query := fmt.Sprintf("INSERT INTO users(userid, draft, inbox, outbox, registeredOn) VALUES ('%s','%s','%s','%s','%s')",
		u.ID, u.Draft, u.Inbox, u.Outbox, u.RegisteredOn)

	_, err := m.db.Exec(query)
	if err != nil {
		d.Subject = "Could not register user."
		res.Mails = append(res.Mails, d)
	} else {
		d.Subject = "User Successfully registered"
		res.Mails = append(res.Mails, d)
	}
	return err
}

type Message struct {
	Sender    string
	Recipient string
	Body      string
	Subject   string
	ID        string
	CreatedAt string
	SentAt    string
}

func queryToInsertMail(m *Mail) string {
	return fmt.Sprintf("INSERT INTO messages(sender, recipient, body, subject, id, createdAt, sentAt) VALUES ('%s','%s','%s','%s','%s','%s','%s')",
		m.Sender, m.Recipient, m.Body, m.Subject, m.Id, time.Now(), "")
}

type User struct {
	ID           string
	Draft        string
	Inbox        string
	Outbox       string
	RegisteredOn string
}

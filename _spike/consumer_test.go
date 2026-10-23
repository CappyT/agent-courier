package spike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// consumerPolicy is the JWT of the ADR: one durable per topic, named
// <key-id>_<topic>, listed in full for create, info, next and ack.
func consumerPolicy(id string, topics ...string) *policy {
	p := &policy{
		pub: []string{"courier.*." + id + ".*", "$JS.API.INFO", "$JS.API.STREAM.INFO.COURIER"},
		sub: []string{"_INBOX_" + id + ".>"},
	}
	for _, t := range topics {
		name := id + "_" + t
		p.pub = append(p.pub,
			"$JS.API.CONSUMER.CREATE.COURIER."+name+".courier."+t+".*.*",
			"$JS.API.CONSUMER.INFO.COURIER."+name,
			"$JS.API.CONSUMER.MSG.NEXT.COURIER."+name,
			"$JS.ACK.COURIER."+name+".>",
		)
	}
	return p
}

func createCourierStream(t *testing.T, sw *swarm) {
	t.Helper()
	data, err := os.ReadFile("../docs/stream.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.MaxAge = 30 * 24 * time.Hour // the file holds nanoseconds; v1 json reads them as is
	js, _ := jetstream.New(sw.adminConn())
	if _, err := js.CreateStream(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
}

func short(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

// denied runs f and expects the server to drop the request for a
// permission violation on subject.
func denied(t *testing.T, c *client, what, subject string, f func() error) {
	t.Helper()
	for len(c.errs) > 0 {
		<-c.errs
	}
	err := f()
	perm := c.permErr(t, 500*time.Millisecond)
	if err == nil {
		t.Fatalf("%s: passed", what)
	}
	if perm == nil || !strings.Contains(perm.Error(), "Permissions Violation for Publish to \""+subject) {
		t.Fatalf("%s: want a publish violation on %s, got request err %v, async %v", what, subject, err, perm)
	}
	t.Logf("%s: denied (%v)", what, perm)
}

func rawCreate(nc *nats.Conn, subject, name string, filters ...string) (string, error) {
	cfg := map[string]any{"durable_name": name, "name": name, "ack_policy": "explicit"}
	if len(filters) == 1 {
		cfg["filter_subject"] = filters[0]
	} else if len(filters) > 1 {
		cfg["filter_subjects"] = filters
	}
	body, _ := json.Marshal(map[string]any{"stream_name": "COURIER", "config": cfg})
	m, err := nc.Request(subject, body, 700*time.Millisecond)
	if err != nil {
		return "", err
	}
	return string(m.Data), nil
}

func TestConsumerExactNames(t *testing.T) {
	sw := startSwarm(t)
	createCourierStream(t, sw)
	sw.setPolicy(func(k string) *policy { return consumerPolicy(k, "hp", "review") })
	a, b := sw.mustConnect(newKey(t)), sw.mustConnect(newKey(t))
	jsA, _ := jetstream.New(a.nc)
	jsB, _ := jetstream.New(b.nc)
	ctx := t.Context()
	nameA := a.id + "_hp"
	nameB := b.id + "_hp"

	// nats.go v1.54 puts a single FilterSubject in the CREATE subject; the
	// JWT allows only that form, so success shows the subject.
	consA, err := jsA.CreateOrUpdateConsumer(ctx, "COURIER", jetstream.ConsumerConfig{
		Durable: nameA, FilterSubject: "courier.hp.*.*", AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("A creates its hp durable: %v", err)
	}
	t.Logf("A created %s with filter in the subject", consA.CachedInfo().Name)
	// FilterSubjects with one entry uses the filterless subject: denied.
	denied(t, a, "A with FilterSubjects=[one]", "$JS.API.CONSUMER.CREATE.COURIER."+a.id+"_review\"", func() error {
		_, err := jsA.CreateOrUpdateConsumer(short(t), "COURIER", jetstream.ConsumerConfig{
			Durable: a.id + "_review", FilterSubjects: []string{"courier.review.*.*"}, AckPolicy: jetstream.AckExplicitPolicy,
		})
		return err
	})

	// B publishes, A fetches and acks.
	if _, err := jsB.Publish(ctx, "courier.hp."+b.id+".note", []byte(`{"summary":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	batch, err := consA.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var msg jetstream.Msg
	for m := range batch.Messages() {
		msg = m
	}
	if msg == nil {
		t.Fatalf("A fetched nothing: %v", batch.Error())
	}
	t.Logf("ack subject: %s", msg.Reply())

	// B against A's durable.
	denied(t, b, "B creates A's durable", "$JS.API.CONSUMER.CREATE.COURIER."+nameA, func() error {
		_, err := jsB.CreateOrUpdateConsumer(short(t), "COURIER", jetstream.ConsumerConfig{
			Durable: nameA, FilterSubject: "courier.hp.*.*", AckPolicy: jetstream.AckExplicitPolicy,
		})
		return err
	})
	denied(t, b, "B reads A's durable info", "$JS.API.CONSUMER.INFO.COURIER."+nameA, func() error {
		_, err := jsB.Consumer(short(t), "COURIER", nameA)
		return err
	})
	denied(t, b, "B fetches from A's durable", "$JS.API.CONSUMER.MSG.NEXT.COURIER."+nameA, func() error {
		_, err := b.nc.Request("$JS.API.CONSUMER.MSG.NEXT.COURIER."+nameA, []byte(`{"batch":1,"expires":300000000}`), 700*time.Millisecond)
		return err
	})
	denied(t, b, "B acks on A's ack subject", msg.Reply(), func() error {
		_, err := b.nc.Request(msg.Reply(), []byte("+ACK"), 700*time.Millisecond)
		return err
	})
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatalf("A acks: %v", err)
	}

	// B on a topic its JWT does not list, through every create subject.
	for _, c := range []struct{ what, subject string }{
		{"filtered CREATE, unlisted topic", "$JS.API.CONSUMER.CREATE.COURIER." + b.id + "_secret.courier.secret.*.*"},
		{"filterless CREATE", "$JS.API.CONSUMER.CREATE.COURIER." + nameB},
		{"legacy DURABLE.CREATE", "$JS.API.CONSUMER.DURABLE.CREATE.COURIER." + nameB},
		{"ephemeral CREATE", "$JS.API.CONSUMER.CREATE.COURIER"},
	} {
		denied(t, b, "B "+c.what, c.subject, func() error {
			_, err := rawCreate(b.nc, c.subject, nameB, "courier.secret.*.*")
			return err
		})
	}
	denied(t, b, "B ordered consumer", "$JS.API.CONSUMER.CREATE.COURIER.", func() error {
		oc, err := jsB.OrderedConsumer(short(t), "COURIER", jetstream.OrderedConsumerConfig{FilterSubjects: []string{"courier.hp.*.*"}})
		if err != nil {
			return err
		}
		_, err = oc.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
		return err
	})

	// B on its own allowed subject, with a body that widens the filter:
	// the server checks the body against the subject.
	subj := "$JS.API.CONSUMER.CREATE.COURIER." + nameB + ".courier.hp.*.*"
	for _, body := range [][]string{{"courier.secret.*.*"}, {"courier.hp.*.*", "courier.secret.*.*"}, {"courier.>"}, nil} {
		resp, err := rawCreate(b.nc, subj, nameB, body...)
		if err != nil {
			t.Fatalf("B own subject, body %v: %v", body, err)
		}
		if !strings.Contains(resp, `"error"`) {
			t.Fatalf("B own subject with body filter %v: server accepted: %s", body, resp)
		}
		t.Logf("B own subject, body filters %v: %s", body, errText(resp))
	}
	resp, err := rawCreate(b.nc, subj, nameB, "courier.hp.*.*")
	if err != nil || strings.Contains(resp, `"error"`) {
		t.Fatalf("B own durable: %v %s", err, resp)
	}
	t.Logf("B own durable with the matching filter: created")
	// B names A's durable in the body of its own allowed subject.
	resp, _ = rawCreate(b.nc, "$JS.API.CONSUMER.CREATE.COURIER."+b.id+"_review.courier.review.*.*", nameA, "courier.review.*.*")
	if !strings.Contains(resp, `"error"`) {
		t.Fatalf("B created A's durable through its own subject: %s", resp)
	}
	t.Logf("B body names A's durable on B's subject: %s", errText(resp))
	// B tries to rewrite its own durable into a wider one on the same subject.
	resp, _ = rawCreate(b.nc, subj, nameB, "courier.*.*.*")
	t.Logf("B widens own durable via update: %s", errText(resp))
}

func errText(resp string) string {
	var r struct {
		Error *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	_ = json.Unmarshal([]byte(resp), &r)
	if r.Error == nil {
		return "ok"
	}
	return fmt.Sprintf("error %d/%d %s", r.Error.Code, r.Error.ErrCode, r.Error.Description)
}

var _ = errors.Is

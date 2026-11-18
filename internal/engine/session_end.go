package engine

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// willMessage is a session's Will Message as given at CONNECT.
type willMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	Props   []byte
}

// sessionEnd describes a session whose network connection is over: which
// row it is, who must still own it, and what the client asked for at
// CONNECT (or changed at DISCONNECT).
type sessionEnd struct {
	ClientID string
	Token    uuid.UUID
	// Owner is the broker the row must still name. A row that has been
	// handed to another connection, or already ended, does not match.
	Owner uuid.UUID
	// Expiry is how long the session outlives its connection: nil never
	// expires, 0 ends with the connection, n lasts n seconds.
	Expiry *uint32
	// Will is nil when there is none or the client discarded it with a
	// normal DISCONNECT.
	Will      *willMessage
	WillDelay uint32
}

// endSessionTimeout bounds one endSession. It is generous because a
// broker shutting down ends every session it holds at once, through one
// pool; a session that misses it is ended by a peer's janitor instead.
const endSessionTimeout = 15 * time.Second

// endSession is the one place a session's connection ending is recorded.
// Whatever ended it (the client went away, the server closed it, its
// broker died), the same things have to happen and they have to happen
// together:
//
//   - the Will Message is published now, or scheduled if a Will Delay
//     applies [MQTT-3.1.2-8];
//   - a session that does not outlive its connection is deleted, and one
//     that does gets its expiry time;
//   - the row stops naming a broker.
//
// It all runs in one transaction that starts by locking the row on
// (client, token, owner). If the row no longer matches, a newer
// connection owns the session and nothing is done. If the will cannot be
// published the transaction rolls back and the row is left exactly as it
// was, still owned, for the next attempt: a will is never dropped because
// publishing it failed, and never published for a session that was not
// ended.
//
// Returns whether the session was ended by this call.
func (e *Engine) endSession(ctx context.Context, s sessionEnd) (bool, error) {
	tx, err := e.beginTxTimed(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var one int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM sessions
		 WHERE client_id=$1 AND session_token=$2 AND broker_id=$3
		   FOR UPDATE
	`, s.ClientID, s.Token, s.Owner).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	ends := s.Expiry != nil && *s.Expiry == 0
	// [MQTT-3.1.3-9]: the will waits for the Will Delay or the end of the
	// session, whichever comes first.
	delay := s.WillDelay
	if s.Expiry != nil && *s.Expiry < delay {
		delay = *s.Expiry
	}

	var (
		willRes   publishResult
		published bool
	)
	if s.Will != nil && delay == 0 {
		willRes, err = e.publishTx(ctx, tx, publishCore{
			Topic:      s.Will.Topic,
			Payload:    s.Will.Payload,
			QoS:        s.Will.QoS,
			Retain:     s.Will.Retain,
			Properties: s.Will.Props,
		})
		if err != nil {
			return false, err
		}
		published = true
	}

	if ends {
		// deliveries does not cascade from sessions (migration 0006).
		if _, err := tx.Exec(ctx, `DELETE FROM deliveries WHERE client_id=$1`, s.ClientID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM sessions WHERE client_id=$1`, s.ClientID); err != nil {
			return false, err
		}
	} else {
		keepWill := s.Will != nil && !published
		if _, err := tx.Exec(ctx, `
			UPDATE sessions SET
				connected=false,
				broker_id=NULL,
				last_seen=now(),
				will_fire_at       = CASE WHEN $2 THEN now() + $3::bigint * interval '1 second' END,
				session_expires_at = CASE WHEN $4 THEN now() + $5::bigint * interval '1 second' END,
				will_topic      = CASE WHEN $2 THEN will_topic END,
				will_payload    = CASE WHEN $2 THEN will_payload END,
				will_qos        = CASE WHEN $2 THEN will_qos END,
				will_retain     = CASE WHEN $2 THEN will_retain END,
				will_delay      = CASE WHEN $2 THEN will_delay END,
				will_properties = CASE WHEN $2 THEN will_properties END
			 WHERE client_id=$1
		`, s.ClientID, keepWill, int64(delay), s.Expiry != nil, int64(expirySeconds(s.Expiry))); err != nil {
			return false, err
		}
		// QoS 0 is not queued for a session with no connection, so what
		// was queued for this one and not yet sent goes with it.
		if _, err := tx.Exec(ctx,
			`DELETE FROM deliveries WHERE client_id=$1 AND qos=0 AND state=0`, s.ClientID); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	if published {
		e.afterWillPublished(ctx, willRes)
	}
	return true, nil
}

func expirySeconds(expiry *uint32) uint32 {
	if expiry == nil {
		return 0
	}
	return *expiry
}

// afterWillPublished runs the post-commit half of a will publish.
func (e *Engine) afterWillPublished(ctx context.Context, res publishResult) {
	e.observeOverflow(res)
	if e.metrics != nil {
		e.metrics.WillsFiredTotal.Inc()
	}
	if err := e.notify.Notify(ctx, res.BrokerIDs, res.MessageID); err != nil {
		// In production the post-commit Notifier hook is a no-op
		// (publishTx emits pg_notify in-tx) so this counter normally
		// stays at zero. In tests using InProcessNotifier it surfaces
		// rig-level cross-pod deliver failures.
		e.logger.Error("will notify failed; cross-pod subs may miss this will until next NOTIFY for this broker",
			"msg", res.MessageID, "brokers", res.BrokerIDs, "err", err)
		if e.metrics != nil {
			e.metrics.WillsNotifyFailedTotal.Inc()
		}
	}
	if len(res.OverflowClients) > 0 {
		e.dispatchQuotaExceeded(ctx, res.OverflowClients)
	}
}

// sessionEnd builds the description of this connection's session from
// what the client sent, including anything its DISCONNECT changed.
func (c *Conn) sessionEnd() sessionEnd {
	s := sessionEnd{
		ClientID: c.clientID,
		Token:    c.sessionToken,
		Owner:    c.eng.BrokerID(),
	}
	switch c.protocol {
	case mqttwire.ProtocolMQTT5:
		// [MQTT-3.1.2-23]: an absent Session Expiry Interval is 0, and
		// 0xFFFFFFFF means the session does not expire.
		var v uint32
		if c.sessionExpiry != nil {
			v = *c.sessionExpiry
		}
		if v != math.MaxUint32 {
			s.Expiry = &v
		}
		if c.willDelay != nil {
			s.WillDelay = *c.willDelay
		}
	default:
		if c.cleanStart {
			var zero uint32
			s.Expiry = &zero
		}
	}
	if c.willTopic != "" {
		s.Will = &willMessage{
			Topic:   c.willTopic,
			Payload: c.willPayload,
			QoS:     c.willQoS,
			Retain:  c.willRetain,
			Props:   c.willProps,
		}
	}
	return s
}

// sessionEndFromRow is sessionEnd for a session nobody holds in memory:
// its broker died, or lost track of it.
func sessionEndFromRow(owner uuid.UUID, r sessionRow) sessionEnd {
	s := sessionEnd{ClientID: r.clientID, Token: r.token, Owner: owner}
	// expiry_interval holds 0 for a v3.1.1 clean session and NULL for a
	// persistent one; for v5 it is the interval clamped to MaxInt32,
	// which is how 0xFFFFFFFF ("never") is stored.
	if r.expiry != nil && *r.expiry < math.MaxInt32 {
		v := uint32(max(*r.expiry, 0))
		s.Expiry = &v
	}
	if r.willTopic != nil {
		s.Will = &willMessage{
			Topic:   *r.willTopic,
			Payload: r.willPayload,
			Retain:  r.willRetain != nil && *r.willRetain,
			Props:   r.willProps,
		}
		if r.willQoS != nil {
			s.Will.QoS = byte(*r.willQoS)
		}
		if r.willDelay != nil {
			s.WillDelay = uint32(max(*r.willDelay, 0))
		}
	}
	return s
}

type sessionRow struct {
	clientID    string
	token       uuid.UUID
	expiry      *int32
	willTopic   *string
	willPayload []byte
	willQoS     *int16
	willRetain  *bool
	willProps   []byte
	willDelay   *int32
}

const sessionRowColumns = `client_id, session_token, expiry_interval,
	will_topic, will_payload, will_qos, will_retain, will_properties, will_delay`

func scanSessionRows(rows pgx.Rows) ([]sessionRow, error) {
	defer rows.Close()
	var out []sessionRow
	for rows.Next() {
		var r sessionRow
		if err := rows.Scan(&r.clientID, &r.token, &r.expiry,
			&r.willTopic, &r.willPayload, &r.willQoS, &r.willRetain, &r.willProps, &r.willDelay); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// EndSessionsOfBroker ends every session still owned by brokerID, which
// the caller has established is dead and holds the reap lock for. Returns
// how many it ended. A session that fails is logged and left owned, so
// the next reap attempt finds it again.
func (e *Engine) EndSessionsOfBroker(ctx context.Context, brokerID uuid.UUID) (int, error) {
	rows, err := e.pool.Query(ctx,
		`SELECT `+sessionRowColumns+` FROM sessions WHERE broker_id=$1`, brokerID)
	if err != nil {
		return 0, err
	}
	sessions, err := scanSessionRows(rows)
	if err != nil {
		return 0, err
	}
	ended := 0
	for _, r := range sessions {
		ok, err := e.endSession(ctx, sessionEndFromRow(brokerID, r))
		if err != nil {
			e.logger.Warn("end session of dead broker", "broker", brokerID, "client", r.clientID, "err", err)
			continue
		}
		if ok {
			ended++
		}
	}
	return ended, nil
}

// orphanGrace is how long a row must have named this broker before a
// missing connection counts as lost rather than still being set up:
// CONNECT writes the row before it registers the Conn.
const orphanGrace = 2 * connectTimeout

// endOrphanedSessions ends sessions whose row names this broker but that
// no local connection holds. Such a row is left behind whenever the end
// of a connection could not be recorded (Postgres was unreachable at that
// moment), and without this nothing would ever fire its will or let it
// expire while this broker lives.
func (e *Engine) endOrphanedSessions(ctx context.Context) error {
	self := e.BrokerID()
	if self == uuid.Nil {
		return nil
	}
	rows, err := e.pool.Query(ctx, `
		SELECT `+sessionRowColumns+` FROM sessions
		 WHERE broker_id=$1 AND last_seen < now() - $2::float8 * interval '1 second'
	`, self, orphanGrace.Seconds())
	if err != nil {
		return err
	}
	sessions, err := scanSessionRows(rows)
	if err != nil {
		return err
	}
	for _, r := range sessions {
		if c, ok := e.ConnFor(r.clientID); ok && c.sessionToken == r.token {
			continue
		}
		ended, err := e.endSession(ctx, sessionEndFromRow(self, r))
		if err != nil {
			e.logger.Warn("end orphaned session", "client", r.clientID, "err", err)
			continue
		}
		if ended {
			e.logger.Warn("ended a session this broker owned but held no connection for", "client", r.clientID)
		}
	}
	return nil
}

// FireDueWills publishes every scheduled will whose time has come, each
// in its own transaction with the clearing of its row, so one will
// failing to publish neither loses that will nor holds up the others.
// Returns how many were published.
func (e *Engine) FireDueWills(ctx context.Context) (int, error) {
	rows, err := e.pool.Query(ctx, `
		SELECT client_id FROM sessions
		 WHERE will_fire_at <= now() AND will_topic IS NOT NULL AND NOT connected
	`)
	if err != nil {
		return 0, err
	}
	due, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	fired := 0
	for _, clientID := range due {
		ok, err := e.fireDueWill(ctx, clientID)
		if err != nil {
			e.logger.Warn("fire delayed will", "client", clientID, "err", err)
			continue
		}
		if ok {
			fired++
		}
	}
	return fired, nil
}

func (e *Engine) fireDueWill(ctx context.Context, clientID string) (bool, error) {
	tx, err := e.beginTxTimed(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	// SKIP LOCKED: another janitor has this one. NOT connected: a client
	// that came back owns the row again, and its will_* are the new
	// connection's.
	var (
		w      willMessage
		qos    int16
		retain bool
		fireAt time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT will_topic, will_payload, will_qos, will_retain, will_properties, will_fire_at
		  FROM sessions
		 WHERE client_id=$1 AND will_fire_at <= now() AND will_topic IS NOT NULL AND NOT connected
		   FOR UPDATE SKIP LOCKED
	`, clientID).Scan(&w.Topic, &w.Payload, &qos, &retain, &w.Props, &fireAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	w.QoS, w.Retain = byte(qos), retain

	res, err := e.publishTx(ctx, tx, publishCore{
		Topic: w.Topic, Payload: w.Payload, QoS: w.QoS, Retain: w.Retain, Properties: w.Props,
	})
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET will_fire_at=NULL,
		    will_topic=NULL, will_payload=NULL, will_qos=NULL,
		    will_retain=NULL, will_delay=NULL, will_properties=NULL
		 WHERE client_id=$1
	`, clientID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	e.afterWillPublished(ctx, res)
	if e.metrics != nil {
		e.metrics.WillFireLatenessSeconds.Observe(max(time.Since(fireAt).Seconds(), 0))
	}
	return true, nil
}

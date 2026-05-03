package engine

import "context"

// fireWill synthesises a publish from the current connection's will fields and
// runs it through the publisher path. It is invoked from the ungraceful
// disconnect handler. The publisher_client_id is *not* set so no_local clients
// also receive the will (they were the publisher, but the publisher has died).
func (c *Conn) fireWill(ctx context.Context) error {
	if c.willTopic == "" {
		return nil
	}
	return c.eng.PublishWill(ctx, c.willTopic, c.willPayload, c.willQoS, c.willRetain, c.willProps)
}

// PublishWill is invoked by both the per-Conn ungraceful path and by the
// janitor's dead-broker scan. It runs the publisher path with no publisher
// client id (so no_local-blocking subscribers still receive the will).
func (e *Engine) PublishWill(ctx context.Context, topic string, payload []byte, qos byte, retain bool, props []byte) error {
	res, err := e.publishCore(ctx, publishCore{
		Topic:      topic,
		Payload:    payload,
		QoS:        qos,
		Retain:     retain,
		Properties: props,
		Publisher:  "",
	})
	if err != nil {
		return err
	}
	if err := e.notify.Notify(ctx, res.BrokerIDs, res.MessageID); err != nil {
		// In production the post-commit Notifier hook is a no-op
		// (publishCore emits pg_notify in-tx) so this counter normally
		// stays at zero. In tests using InProcessNotifier it surfaces
		// rig-level cross-pod deliver failures. Either way: an operator
		// alert on rate(pgmqtt_wills_notify_failed_total[5m]) > 0 is a
		// signal worth investigating because at-least-once delivery for
		// wills is no longer guaranteed for the cross-pod subs.
		e.logger.Error("will notify failed; cross-pod subs may miss this will until next NOTIFY for this broker",
			"msg", res.MessageID, "brokers", res.BrokerIDs, "err", err)
		if e.metrics != nil {
			e.metrics.WillsNotifyFailedTotal.Inc()
		}
	}
	if len(res.OverflowClients) > 0 {
		e.dispatchQuotaExceeded(ctx, res.OverflowClients)
	}
	return nil
}

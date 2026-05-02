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
	msgID, brokerIDs, err := c.eng.publishCore(ctx, publishCore{
		Topic:      c.willTopic,
		Payload:    c.willPayload,
		QoS:        c.willQoS,
		Retain:     c.willRetain,
		Properties: c.willProps,
		Publisher:  "",
	})
	if err != nil {
		return err
	}
	if err := c.eng.notify.Notify(ctx, brokerIDs, msgID); err != nil {
		c.eng.logger.Warn("will notify", "msg", msgID, "err", err)
	}
	return nil
}

package webhook

import (
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/webhook"
)

func TestReplayableDelivery(t *testing.T) {
	revision, laterRevision := int64(1), int64(2)
	httpStatus := 400
	finishedAt := time.Unix(1, 0)
	base := app.DeliveryInfo{
		EndpointRevision: &revision, Status: "dead_letter", Attempts: 1,
		LastHTTPStatus: &httpStatus, LastErrorCode: app.CodeHTTPPermanent, CompletedAt: &finishedAt,
	}
	for _, test := range []struct {
		name   string
		change func(*app.DeliveryInfo)
		want   bool
	}{
		{"permanent HTTP response", nil, true},
		{"exhausted attempts", func(item *app.DeliveryInfo) {
			item.Attempts = 8
			item.LastHTTPStatus = nil
			item.LastErrorCode = app.CodeDeliveryDeadLetter
		}, true},
		{"obsolete revision", func(item *app.DeliveryInfo) { item.EndpointRevision = &laterRevision }, false},
		{"legacy delivery", func(item *app.DeliveryInfo) { item.EndpointRevision = nil }, false},
		{"unfinished", func(item *app.DeliveryInfo) { item.CompletedAt = nil }, false},
		{"delivered", func(item *app.DeliveryInfo) { item.Status = "delivered" }, false},
		{"cancelled", func(item *app.DeliveryInfo) { item.Status = "cancelled" }, false},
		{"missing attempt", func(item *app.DeliveryInfo) { item.Attempts = 0 }, false},
		{"over the attempt limit", func(item *app.DeliveryInfo) { item.Attempts = 9 }, false},
		{"invalid secret", func(item *app.DeliveryInfo) { item.LastErrorCode = app.CodeSecretInvalid }, false},
		{"invalid protocol", func(item *app.DeliveryInfo) { item.LastErrorCode = app.CodeProtocolInvalid }, false},
		{"missing HTTP response", func(item *app.DeliveryInfo) { item.LastHTTPStatus = nil }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			item := base
			if test.change != nil {
				test.change(&item)
			}
			if got := replayable(item, &revision); got != test.want {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
}

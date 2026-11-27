package webhook

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/webhook"
)

const deliverySelect = `SELECT delivery_id,event_id,destination_id,endpoint_revision,status,attempts,
last_http_status,COALESCE(last_error_code,''),next_attempt_at,completed_at,updated_at
FROM newim.im_webhook_deliveries`

func scanDelivery(row pgx.Row) (app.DeliveryInfo, error) {
	var item app.DeliveryInfo
	var code string
	err := row.Scan(&item.DeliveryID, &item.EventID, &item.DestinationID, &item.EndpointRevision, &item.Status, &item.Attempts,
		&item.LastHTTPStatus, &code, &item.NextAttemptAt, &item.CompletedAt, &item.UpdatedAt)
	item.LastErrorCode = app.Code(code)
	return item, err
}

func (m *Management) ListDeliveries(ctx context.Context, query app.DeliveryQuery) (app.DeliveryPage, error) {
	if ctx == nil || !query.Valid() {
		return app.DeliveryPage{}, app.Fail(app.CodeManagementInput)
	}
	if !m.available() {
		return app.DeliveryPage{}, app.Fail(app.CodeStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	if query.DeliveryID != "" {
		item, err := scanDelivery(m.repo.pool.QueryRow(ctx, deliverySelect+`
WHERE destination_id=$1 AND delivery_id=$2`, query.DestinationID, query.DeliveryID))
		if errors.Is(err, pgx.ErrNoRows) {
			return app.DeliveryPage{}, app.Fail(app.CodeManagementNotFound)
		}
		if err != nil {
			return app.DeliveryPage{}, mapStorageError(err)
		}
		return app.DeliveryPage{Items: []app.DeliveryInfo{item}}, nil
	}
	rows, err := m.repo.pool.Query(ctx, deliverySelect+`
WHERE destination_id=$1 AND delivery_id>$2::text ORDER BY delivery_id LIMIT $3`,
		query.DestinationID, query.AfterID, query.Limit+1)
	if err != nil {
		return app.DeliveryPage{}, mapStorageError(err)
	}
	defer rows.Close()
	page := app.DeliveryPage{Items: make([]app.DeliveryInfo, 0, query.Limit)}
	for rows.Next() {
		item, err := scanDelivery(rows)
		if err != nil {
			return app.DeliveryPage{}, mapStorageError(err)
		}
		page.Items = append(page.Items, item)
	}
	if err = rows.Err(); err != nil {
		return app.DeliveryPage{}, mapStorageError(err)
	}
	if len(page.Items) > query.Limit {
		page.Items = page.Items[:query.Limit]
		page.NextAfterID = page.Items[query.Limit-1].DeliveryID
	}
	return page, nil
}

func (m *Management) ReplayDelivery(ctx context.Context, request app.ReplayRequest) (app.ReplayReceipt, error) {
	if ctx == nil || !request.Valid() {
		return app.ReplayReceipt{}, app.Fail(app.CodeManagementInput)
	}
	if !m.available() {
		return app.ReplayReceipt{}, app.Fail(app.CodeStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := m.repo.begin(ctx)
	if err != nil {
		return app.ReplayReceipt{}, err
	}
	defer rollback(tx)
	if err = lockFanout(ctx, tx); err != nil {
		return app.ReplayReceipt{}, err
	}
	receipt, err := m.replayDelivery(ctx, tx, request)
	if err != nil {
		return app.ReplayReceipt{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return app.ReplayReceipt{}, mapStorageError(err)
	}
	return receipt, nil
}

func (m *Management) replayDelivery(ctx context.Context, tx pgx.Tx, request app.ReplayRequest) (app.ReplayReceipt, error) {
	var receipt app.ReplayReceipt
	err := tx.QueryRow(ctx, `SELECT request_id,destination_id,delivery_id,revision,recorded_at
FROM newim.im_webhook_management_audit WHERE action='replay' AND request_id=$1`, request.RequestID).
		Scan(&receipt.RequestID, &receipt.DestinationID, &receipt.DeliveryID, &receipt.Revision, &receipt.ReplayedAt)
	if err == nil {
		if receipt.DeliveryID != request.DeliveryID || receipt.DestinationID != request.DestinationID {
			return app.ReplayReceipt{}, app.Fail(app.CodeManagementConflict)
		}
		return receipt, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return app.ReplayReceipt{}, mapStorageError(err)
	}

	var status string
	var revision *int64
	err = tx.QueryRow(ctx, `SELECT status,active_revision FROM newim.im_webhook_endpoints
WHERE destination_id=$1 FOR UPDATE`, request.DestinationID).Scan(&status, &revision)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return app.ReplayReceipt{}, mapStorageError(err)
	}
	item, err := scanDelivery(tx.QueryRow(ctx, deliverySelect+`
WHERE destination_id=$1 AND delivery_id=$2 FOR UPDATE`, request.DestinationID, request.DeliveryID))
	if errors.Is(err, pgx.ErrNoRows) {
		return app.ReplayReceipt{}, app.Fail(app.CodeManagementNotFound)
	}
	if err != nil {
		return app.ReplayReceipt{}, mapStorageError(err)
	}
	if status != "active" || !replayable(item, revision) {
		return app.ReplayReceipt{}, app.Fail(app.CodeManagementNotReplayable)
	}
	var replayed bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM newim.im_webhook_management_audit
WHERE action='replay' AND delivery_id=$1)`, item.DeliveryID).Scan(&replayed); err != nil {
		return app.ReplayReceipt{}, mapStorageError(err)
	}
	if replayed {
		return app.ReplayReceipt{}, app.Fail(app.CodeManagementNotReplayable)
	}
	if err = m.checkReplayCapacity(ctx, tx, item.DestinationID); err != nil {
		return app.ReplayReceipt{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE newim.im_webhook_deliveries
SET status='pending',attempts=0,next_attempt_at=clock_timestamp(),completed_at=NULL,
last_http_status=NULL,last_error_code=NULL,updated_at=GREATEST(clock_timestamp(),updated_at)
WHERE delivery_id=$1`, item.DeliveryID); err != nil {
		return app.ReplayReceipt{}, mapStorageError(err)
	}
	receipt = app.ReplayReceipt{
		RequestID: request.RequestID, DestinationID: item.DestinationID, DeliveryID: item.DeliveryID, Revision: *revision,
	}
	err = tx.QueryRow(ctx, `INSERT INTO newim.im_webhook_management_audit
(action,request_id,destination_id,revision,delivery_id,previous_status,previous_attempts,previous_http_status,
 previous_error_code,previous_completed_at)
VALUES ('replay',$1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING recorded_at`,
		receipt.RequestID, receipt.DestinationID, receipt.Revision, receipt.DeliveryID, item.Status, item.Attempts,
		item.LastHTTPStatus, string(item.LastErrorCode), item.CompletedAt).Scan(&receipt.ReplayedAt)
	if err != nil {
		return app.ReplayReceipt{}, mapStorageError(err)
	}
	return receipt, nil
}

func replayable(item app.DeliveryInfo, activeRevision *int64) bool {
	if activeRevision == nil || item.EndpointRevision == nil || *activeRevision != *item.EndpointRevision ||
		item.Status != "dead_letter" || item.CompletedAt == nil || item.Attempts < 1 || item.Attempts > 8 {
		return false
	}
	return item.LastErrorCode == app.CodeDeliveryDeadLetter ||
		(item.LastErrorCode == app.CodeHTTPPermanent && item.LastHTTPStatus != nil)
}

func (m *Management) checkReplayCapacity(ctx context.Context, tx pgx.Tx, destination string) error {
	var full bool
	err := tx.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM (
    SELECT 1 FROM newim.im_webhook_deliveries WHERE status IN ('pending','retry') LIMIT $1
  ) due) + (SELECT count(*) FROM (
    SELECT 1 FROM newim.im_webhook_deliveries WHERE status='leased' LIMIT $1
  ) leased) >= $1
  OR (SELECT count(*) FROM (
    SELECT 1 FROM newim.im_webhook_deliveries
    WHERE destination_id=$2 AND status IN ('pending','retry','leased') LIMIT $3
  ) target) >= $3`, m.highWater, destination, m.maxDestination).Scan(&full)
	if err != nil {
		return mapStorageError(err)
	}
	if full {
		return app.Fail(app.CodeManagementCapacity)
	}
	return nil
}

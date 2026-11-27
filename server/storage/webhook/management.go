package webhook

import (
	"context"
	"errors"
	"math"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/webhook"
)

// Management performs operator actions through the worker's repository.
type Management struct {
	repo           *Repository
	codec          *app.LocalSecretResolver
	highWater      int
	maxDestination int
}

var _ app.ManagementStore = (*Management)(nil)

func NewManagement(repo *Repository, codec *app.LocalSecretResolver, highWater, maxDestination int) (*Management, error) {
	if repo == nil || repo.pool == nil || codec == nil ||
		highWater < 1 || highWater > 1_000_000 || maxDestination < 1 || maxDestination > 1_000_000 {
		return nil, app.Fail(app.CodeInvalidConfig)
	}
	return &Management{repo: repo, codec: codec, highWater: highWater, maxDestination: maxDestination}, nil
}

func (m *Management) available() bool {
	return m != nil && m.repo != nil && m.repo.pool != nil
}

func (m *Management) CheckSchema(ctx context.Context) error {
	if !m.available() || ctx == nil {
		return app.Fail(app.CodeStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var installed bool
	err := m.repo.pool.QueryRow(ctx, `SELECT to_regclass('newim.im_webhook_management_replay_request_idx') IS NOT NULL
AND to_regclass('newim.im_webhook_management_replay_delivery_idx') IS NOT NULL
AND to_regclass('newim.im_webhook_deliveries_management_idx') IS NOT NULL
AND to_regclass('newim.im_webhook_endpoints_active_idx') IS NOT NULL
AND to_regclass('newim.im_webhook_revisions_key_idx') IS NOT NULL`).Scan(&installed)
	if err != nil || !installed {
		return app.Fail(app.CodeStorageUnavailable)
	}
	_, err = m.repo.pool.Exec(ctx, `SELECT audit_id, actor, action, request_id, destination_id, revision,
recorded_at, delivery_id, previous_status, previous_attempts, previous_http_status, previous_error_code,
previous_completed_at FROM newim.im_webhook_management_audit LIMIT 0`)
	return mapStorageError(err)
}

const endpointSelect = `SELECT e.destination_id,e.status,e.active_revision,COALESCE(v.key_id,''),
e.created_at,e.updated_at,e.revoked_at
FROM newim.im_webhook_endpoints e
LEFT JOIN newim.im_webhook_endpoint_revisions v
  ON v.destination_id=e.destination_id AND v.revision=e.active_revision`

func scanEndpoint(row pgx.Row) (app.EndpointInfo, error) {
	var item app.EndpointInfo
	err := row.Scan(&item.DestinationID, &item.Status, &item.Revision, &item.KeyID, &item.CreatedAt, &item.UpdatedAt, &item.RevokedAt)
	return item, err
}

func (m *Management) GetEndpoint(ctx context.Context, id string) (app.EndpointInfo, error) {
	if ctx == nil || !app.ValidManagementID(id) {
		return app.EndpointInfo{}, app.Fail(app.CodeManagementInput)
	}
	if !m.available() {
		return app.EndpointInfo{}, app.Fail(app.CodeStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	item, err := scanEndpoint(m.repo.pool.QueryRow(ctx, endpointSelect+` WHERE e.destination_id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return app.EndpointInfo{}, app.Fail(app.CodeManagementNotFound)
	}
	return item, mapStorageError(err)
}

func (m *Management) ListEndpoints(ctx context.Context, after string, limit int) (app.EndpointPage, error) {
	if ctx == nil || !app.ValidManagementPage(after, limit) {
		return app.EndpointPage{}, app.Fail(app.CodeManagementInput)
	}
	if !m.available() {
		return app.EndpointPage{}, app.Fail(app.CodeStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	rows, err := m.repo.pool.Query(ctx, endpointSelect+`
WHERE e.destination_id>$1::text ORDER BY e.destination_id LIMIT $2`, after, limit+1)
	if err != nil {
		return app.EndpointPage{}, mapStorageError(err)
	}
	defer rows.Close()
	page := app.EndpointPage{Items: make([]app.EndpointInfo, 0, limit)}
	for rows.Next() {
		item, err := scanEndpoint(rows)
		if err != nil {
			return app.EndpointPage{}, mapStorageError(err)
		}
		page.Items = append(page.Items, item)
	}
	if err = rows.Err(); err != nil {
		return app.EndpointPage{}, mapStorageError(err)
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		page.NextAfterID = page.Items[limit-1].DestinationID
	}
	return page, nil
}

func (m *Management) ChangeEndpoint(ctx context.Context, mutation app.EndpointMutation) (app.EndpointInfo, error) {
	if ctx == nil || !mutation.Valid() {
		return app.EndpointInfo{}, app.Fail(app.CodeManagementInput)
	}
	if !m.available() {
		return app.EndpointInfo{}, app.Fail(app.CodeStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := m.repo.begin(ctx)
	if err != nil {
		return app.EndpointInfo{}, err
	}
	defer rollback(tx)
	if err = lockFanout(ctx, tx); err != nil {
		return app.EndpointInfo{}, err
	}
	if err = m.changeEndpoint(ctx, tx, mutation); err != nil {
		return app.EndpointInfo{}, err
	}
	current, err := scanEndpoint(tx.QueryRow(ctx, endpointSelect+` WHERE e.destination_id=$1`, mutation.DestinationID))
	if err != nil || current.Revision == nil {
		return app.EndpointInfo{}, app.Fail(app.CodeStorageUnavailable)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO newim.im_webhook_management_audit(action,request_id,destination_id,revision)
VALUES ($1,$2,$3,$4)`, string(mutation.Operation), mutation.RequestID, mutation.DestinationID, *current.Revision); err != nil {
		return app.EndpointInfo{}, mapStorageError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.EndpointInfo{}, mapStorageError(err)
	}
	return current, nil
}

func (m *Management) changeEndpoint(ctx context.Context, tx pgx.Tx, mutation app.EndpointMutation) error {
	var status string
	var revision *int64
	err := tx.QueryRow(ctx, `SELECT status,active_revision FROM newim.im_webhook_endpoints
WHERE destination_id=$1 FOR UPDATE`, mutation.DestinationID).Scan(&status, &revision)
	if mutation.Operation == app.CreateEndpoint {
		switch {
		case err == nil:
			return app.Fail(app.CodeManagementConflict)
		case errors.Is(err, pgx.ErrNoRows):
			return m.createEndpoint(ctx, tx, mutation)
		default:
			return mapStorageError(err)
		}
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.CodeManagementNotFound)
	}
	if err != nil {
		return mapStorageError(err)
	}
	if status != "active" || revision == nil || *revision != mutation.ExpectedRevision {
		return app.Fail(app.CodeManagementConflict)
	}
	if mutation.Operation == app.RevokeEndpoint {
		_, err := tx.Exec(ctx, `UPDATE newim.im_webhook_endpoints SET status='revoked',
revoked_at=GREATEST(clock_timestamp(),updated_at), updated_at=GREATEST(clock_timestamp(),updated_at)
WHERE destination_id=$1`, mutation.DestinationID)
		return mapStorageError(err)
	}
	return m.appendRevision(ctx, tx, mutation)
}

func (m *Management) createEndpoint(ctx context.Context, tx pgx.Tx, mutation app.EndpointMutation) error {
	var active int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM (
SELECT 1 FROM newim.im_webhook_endpoints WHERE status='active' AND revoked_at IS NULL LIMIT $1
) active`, app.MaxEndpointsPerEvent).Scan(&active); err != nil {
		return mapStorageError(err)
	}
	if active >= app.MaxEndpointsPerEvent {
		return app.Fail(app.CodeManagementCapacity)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO newim.im_webhook_endpoints(destination_id,status,active_revision)
VALUES ($1,'active',1)`, mutation.DestinationID); err != nil {
		return mapStorageError(err)
	}
	return m.insertRevision(ctx, tx, app.SecretMaterial{
		DestinationID: mutation.DestinationID, Revision: 1, URL: mutation.URL, KeyID: mutation.KeyID,
	}, mutation.Secret)
}

func (m *Management) appendRevision(ctx context.Context, tx pgx.Tx, mutation app.EndpointMutation) error {
	current := app.SecretMaterial{DestinationID: mutation.DestinationID, Revision: mutation.ExpectedRevision}
	if err := tx.QueryRow(ctx, `SELECT url,key_id,secret_nonce,secret_ciphertext
FROM newim.im_webhook_endpoint_revisions WHERE destination_id=$1 AND revision=$2`,
		current.DestinationID, current.Revision).Scan(&current.URL, &current.KeyID, &current.Nonce, &current.Ciphertext); err != nil {
		return mapStorageError(err)
	}
	if mutation.Operation == app.UpdateEndpoint && mutation.URL == current.URL {
		return nil
	}
	if current.Revision == math.MaxInt64 {
		return app.Fail(app.CodeManagementConflict)
	}
	next := app.SecretMaterial{
		DestinationID: current.DestinationID, Revision: current.Revision + 1, URL: current.URL, KeyID: current.KeyID,
	}
	secret := mutation.Secret
	if mutation.Operation == app.UpdateEndpoint {
		var err error
		secret, err = m.codec.Resolve(ctx, current)
		if err != nil {
			return app.Fail(app.CodeStorageUnavailable)
		}
		defer clear(secret)
		next.URL = mutation.URL
	} else {
		var keyUsed bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM newim.im_webhook_endpoint_revisions
WHERE destination_id=$1 AND key_id=$2)`, current.DestinationID, mutation.KeyID).Scan(&keyUsed); err != nil {
			return mapStorageError(err)
		}
		if keyUsed {
			return app.Fail(app.CodeManagementConflict)
		}
		next.KeyID = mutation.KeyID
	}
	if err := m.insertRevision(ctx, tx, next, secret); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE newim.im_webhook_endpoints
SET active_revision=$2,updated_at=GREATEST(clock_timestamp(),updated_at)
WHERE destination_id=$1`, next.DestinationID, next.Revision)
	return mapStorageError(err)
}

func (m *Management) insertRevision(ctx context.Context, tx pgx.Tx, material app.SecretMaterial, secret []byte) error {
	sealed, err := m.codec.SealRevision(material, secret)
	if err != nil {
		return app.Fail(app.CodeStorageUnavailable)
	}
	_, err = tx.Exec(ctx, `INSERT INTO newim.im_webhook_endpoint_revisions
(destination_id,revision,url,key_id,secret_nonce,secret_ciphertext) VALUES ($1,$2,$3,$4,$5,$6)`,
		sealed.DestinationID, sealed.Revision, sealed.URL, sealed.KeyID, sealed.Nonce, sealed.Ciphertext)
	return mapStorageError(err)
}

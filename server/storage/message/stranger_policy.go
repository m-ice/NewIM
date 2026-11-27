package message

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	app "github.com/m-ice/NewIM/server/message"
)

func checkStrangerPolicySchema(ctx context.Context, pool *pgxpool.Pool) error {
	var indexReady bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_index AS i
		JOIN pg_catalog.pg_class AS idx ON idx.oid=i.indexrelid
		JOIN pg_catalog.pg_am AS am ON am.oid=idx.relam
		JOIN pg_catalog.pg_opclass AS text_ops
		  ON text_ops.opcmethod=am.oid AND text_ops.opcnamespace='pg_catalog'::regnamespace
		  AND text_ops.opcname='text_ops' AND text_ops.opcdefault
		JOIN pg_catalog.pg_attribute AS conversation
		  ON conversation.attrelid=i.indrelid AND conversation.attnum=i.indkey[0]
		JOIN pg_catalog.pg_attribute AS sender
		  ON sender.attrelid=i.indrelid AND sender.attnum=i.indkey[1]
		WHERE i.indexrelid=pg_catalog.to_regclass('newim.im_messages_conversation_sender_idx')
		  AND i.indrelid='newim.im_messages'::regclass AND am.amname='btree'
		  AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
		  AND i.indnatts=2 AND i.indnkeyatts=2 AND i.indpred IS NULL
		  AND i.indclass[0]=text_ops.oid AND i.indclass[1]=text_ops.oid
		  AND i.indoption[0]=0 AND i.indoption[1]=0
		  AND conversation.attname='conversation_id' AND sender.attname='sender_id'
		  AND i.indcollation[0]=conversation.attcollation AND i.indcollation[1]=sender.attcollation
	)`).Scan(&indexReady)
	if err != nil || !indexReady {
		return app.Fail(app.SendStorageUnavailable)
	}
	if _, err = pool.Exec(ctx, "SELECT owner_id,peer_id FROM newim.im_friend_edges LIMIT 0"); err != nil {
		return app.Fail(app.SendStorageUnavailable)
	}
	return nil
}

// The caller holds the direct conversation and account-pair locks through commit.
func checkStrangerSend(ctx context.Context, tx pgx.Tx, conversationID, senderID, peerID string) error {
	var waitingForReply bool
	// Seek to each key before testing equality. An unordered EXISTS can choose a
	// table scan for the absent side of a long, one-way conversation.
	err := tx.QueryRow(ctx, `SELECT COALESCE((
		SELECT ROW(m.conversation_id,m.sender_id)=ROW($1,$2)
		FROM newim.im_messages AS m
		WHERE ROW(m.conversation_id,m.sender_id)>=ROW($1,$2)
		ORDER BY m.conversation_id,m.sender_id LIMIT 1
	),false) AND NOT COALESCE((
		SELECT ROW(m.conversation_id,m.sender_id)=ROW($1,$3)
		FROM newim.im_messages AS m
		WHERE ROW(m.conversation_id,m.sender_id)>=ROW($1,$3)
		ORDER BY m.conversation_id,m.sender_id LIMIT 1
	),false) AND NOT (
		EXISTS (SELECT 1 FROM newim.im_friend_edges WHERE owner_id=$2 AND peer_id=$3)
		AND EXISTS (SELECT 1 FROM newim.im_friend_edges WHERE owner_id=$3 AND peer_id=$2)
	)`, conversationID, senderID, peerID).Scan(&waitingForReply)
	if err != nil {
		return privateStorageError(err)
	}
	if waitingForReply {
		return app.Fail(app.StrangerMessageLimit)
	}
	return nil
}

//go:build integration

package media_test

import (
	"testing"
)

func TestMediaDB(t *testing.T) {
	f := openFixture(t)
	if got := f.scalarInt64("SELECT count(*) FROM newim_meta.migrations"); got != 6 {
		t.Fatalf("migration ledger got %d want 6", got)
	}
	if got := f.scalarInt64("SELECT max(version) FROM newim_meta.migrations"); got != 6 {
		t.Fatalf("migration head got %d want 6", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM pg_tables WHERE schemaname='newim'"); got != 18 {
		t.Fatalf("table count got %d want 18", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM pg_tables WHERE schemaname='newim' AND tablename='im_media_assets'"); got != 1 {
		t.Fatalf("media table count got %d want 1", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM information_schema.columns WHERE table_schema='newim' AND table_name='im_media_assets' AND (column_name ILIKE '%url%' OR column_name ILIKE '%object%' OR column_name ILIKE '%public%' OR column_name ILIKE '%acl%' OR column_name ILIKE '%list%')"); got != 0 {
		t.Fatalf("media URL/object columns got %d want 0", got)
	}
	columns := f.scalarString("SELECT string_agg(column_name,',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema='newim' AND table_name='im_media_assets'")
	if columns != "media_key,owner_user_id,device_id,session_id,connection_id,token_id,conversation_id,media_kind,content_type,declared_size_bytes,actual_size_bytes,sha256,upload_grant_id,upload_grant_digest,upload_expires_at,completed_at,state" {
		t.Fatalf("unexpected media columns: %s", columns)
	}
	if got := f.scalarInt64("SELECT count(*) FROM information_schema.columns WHERE table_schema='newim' AND table_name='im_media_assets' AND column_name IN ('owner_user_id','device_id','session_id','connection_id','token_id') AND is_nullable='NO'"); got != 5 {
		t.Fatalf("trusted identity columns got %d want 5 NOT NULL", got)
	}
	fks := f.scalarString("SELECT string_agg(conname||'|'||pg_get_constraintdef(oid), E'\\n' ORDER BY conname) FROM pg_constraint WHERE conrelid='newim.im_media_assets'::regclass AND contype='f'")
	wantFKs := "im_media_assets_device_fk|FOREIGN KEY (owner_user_id, device_id) REFERENCES newim.im_devices(user_id, device_id)\n" +
		"im_media_assets_membership_fk|FOREIGN KEY (conversation_id, owner_user_id) REFERENCES newim.im_conversation_members(conversation_id, user_id)\n" +
		"im_media_assets_session_identity_fk|FOREIGN KEY (owner_user_id, device_id, session_id) REFERENCES newim.im_sessions(user_id, device_id, session_id)\n" +
		"im_media_assets_token_session_fk|FOREIGN KEY (token_id, session_id) REFERENCES newim.im_auth_tokens(token_id, session_id)"
	if fks != wantFKs {
		t.Fatalf("media FK bindings got:\n%s\nwant:\n%s", fks, wantFKs)
	}
	if got := f.scalarInt64("SELECT count(*) FROM pg_constraint WHERE connamespace='newim'::regnamespace AND contype='f' AND confdeltype<>'a'"); got != 0 {
		t.Fatalf("cascading deletes got %d want 0", got)
	}
}

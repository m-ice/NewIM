package message

import (
	"context"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/session"
	media "github.com/m-ice/NewIM/server/media"
)

// MediaValidator is the narrow pre-persistence authorization seam for media.
// MediaValidator 是媒体消息持久化前的窄授权边界。
type MediaValidator interface {
	ValidateForSend(context.Context, session.ConnectionIdentity, string, media.Metadata) error
}

func (s *Service) validateMedia(ctx context.Context, identity session.ConnectionIdentity, request protocol.Send) error {
	if request.Type != "media" || request.Version != 1 {
		return nil
	}
	if s == nil || s.mediaValidator == nil {
		return Fail(SendInvalidInput)
	}
	wireMetadata, err := protocol.DecodeMediaPayload(request.Payload)
	if err != nil {
		return Fail(SendInvalidInput)
	}
	digest, ok := media.ParseSHA256(wireMetadata.SHA256)
	if !ok {
		return Fail(SendInvalidInput)
	}
	metadata := media.Metadata{
		MediaKey: wireMetadata.MediaKey, Kind: wireMetadata.Kind, ContentType: wireMetadata.ContentType,
		Size: parseMediaSize(wireMetadata.Size), SHA256: digest,
	}
	if !media.ValidMetadata(metadata) {
		return Fail(SendInvalidInput)
	}
	validateCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	if err = s.mediaValidator.ValidateForSend(validateCtx, identity, request.ConversationID, metadata); err != nil {
		switch media.ErrorCode(err) {
		case media.MediaStorageUnavailable:
			return Fail(SendStorageUnavailable)
		case media.MediaUnauthorized, media.MediaNotFound, media.MediaExpired, media.MediaConflict, media.MediaInvalidToken:
			return Fail(SendUnauthorized)
		default:
			return Fail(SendInvalidInput)
		}
	}
	return nil
}

func parseMediaSize(value string) int64 {
	if !protocol.ValidMediaSize(value) {
		return 0
	}
	var result int64
	for i := 0; i < len(value); i++ {
		result = result*10 + int64(value[i]-'0')
	}
	return result
}

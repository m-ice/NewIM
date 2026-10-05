package webhook

import (
	"sort"
)

// Operation is one code-owned webhook observation point.
type Operation string

const (
	OperationCycle     Operation = "cycle"
	OperationQueue     Operation = "queue"
	OperationFanout    Operation = "fanout"
	OperationClaim     Operation = "claim"
	OperationAttempt   Operation = "attempt"
	OperationSecret    Operation = "secret"
	OperationEnvelope  Operation = "envelope"
	OperationSignature Operation = "signature"
	OperationDelivery  Operation = "delivery"
	OperationFinish    Operation = "finish"
)

var allOperations = []Operation{
	OperationCycle, OperationQueue, OperationFanout, OperationClaim, OperationAttempt,
	OperationSecret, OperationEnvelope, OperationSignature, OperationDelivery, OperationFinish,
}

var allCodes = []Code{
	CodeInvalidConfig, CodeStorageUnavailable, CodeLeaseLost, CodeSecretUnavailable, CodeSecretInvalid,
	CodeHTTPTemporary, CodeHTTPPermanent, CodeResponseTooLarge, CodeBacklogPaused, CodeEndpointRevoked,
	CodeDeliveryDeadLetter, CodeProtocolInvalid, CodeDeliveryCancelled, CodeDeliveryDelivered,
	CodeDeliveryRetry, CodeFanoutLimit, CodeFanoutSuccess, CodeClaimSuccess, CodeAttemptSuccess,
	CodeFinishSuccess, CodeUnknown,
}

var operationStages = map[Operation]string{
	OperationCycle:     "enqueued",
	OperationQueue:     "enqueued",
	OperationFanout:    "fanout",
	OperationClaim:     "claimed",
	OperationAttempt:   "attempt",
	OperationSecret:    "attempt",
	OperationEnvelope:  "attempt",
	OperationSignature: "attempt",
	OperationDelivery:  "attempt",
	OperationFinish:    "finished",
}

// AllOperations returns the complete code-owned operation set.
func AllOperations() []Operation {
	return append([]Operation(nil), allOperations...)
}

// AllCodes returns the complete code-owned webhook code set.
func AllCodes() []Code {
	return append([]Code(nil), allCodes...)
}

// ClassifyObservation maps every operation/code to fixed bounded labels.
func ClassifyObservation(observation Observation) Observation {
	operation := Operation(observation.Operation)
	stage, ok := operationStages[operation]
	if !ok {
		return Observation{
			Operation: "unknown",
			Stage:     "unknown",
			Result:    "unknown",
			Code:      CodeUnknown,
			Elapsed:   observation.Elapsed,
			Total:     observation.Total, MaxDestination: observation.MaxDestination,
			Pending: observation.Pending, Retry: observation.Retry, Leased: observation.Leased, DeadLetter: observation.DeadLetter,
		}
	}
	code := observation.Code
	if !knownCode(code) {
		code = CodeUnknown
	}
	result := resultForCode(code)
	stage = stageForCode(stage, code, operation)
	return Observation{
		Operation: string(operation), Stage: stage, Result: result, Code: code,
		Elapsed: observation.Elapsed, Total: observation.Total, MaxDestination: observation.MaxDestination,
		Pending: observation.Pending, Retry: observation.Retry, Leased: observation.Leased, DeadLetter: observation.DeadLetter,
	}
}

func knownCode(code Code) bool {
	for _, candidate := range allCodes {
		if code == candidate {
			return true
		}
	}
	return false
}

func resultForCode(code Code) string {
	switch code {
	case "", CodeFanoutSuccess, CodeClaimSuccess, CodeAttemptSuccess, CodeFinishSuccess, CodeDeliveryDelivered:
		return "success"
	case CodeStorageUnavailable, CodeLeaseLost, CodeSecretUnavailable, CodeHTTPTemporary, CodeBacklogPaused, CodeDeliveryRetry:
		return "retryable"
	case CodeInvalidConfig, CodeSecretInvalid, CodeHTTPPermanent, CodeResponseTooLarge, CodeEndpointRevoked,
		CodeDeliveryDeadLetter, CodeProtocolInvalid, CodeDeliveryCancelled, CodeFanoutLimit:
		return "permanent"
	default:
		return "unknown"
	}
}

func stageForCode(stage string, code Code, operation Operation) string {
	switch code {
	case CodeDeliveryDelivered, CodeFinishSuccess:
		if operation == OperationFinish {
			return "delivery_success"
		}
	case CodeDeliveryRetry, CodeHTTPTemporary, CodeBacklogPaused, CodeSecretUnavailable:
		if operation == OperationFinish {
			return "delivery_retry"
		}
	case CodeDeliveryDeadLetter, CodeHTTPPermanent, CodeResponseTooLarge, CodeSecretInvalid,
		CodeProtocolInvalid, CodeEndpointRevoked, CodeDeliveryCancelled:
		if operation == OperationFinish {
			return "delivery_dead_letter"
		}
	}
	return stage
}

// ObservationOperationsSorted returns deterministic enumeration for completeness tests.
func ObservationOperationsSorted() []string {
	values := make([]string, 0, len(allOperations))
	for _, operation := range allOperations {
		values = append(values, string(operation))
	}
	sort.Strings(values)
	return values
}

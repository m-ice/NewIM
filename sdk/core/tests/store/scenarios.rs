use newim_sdk_core::store::*;
/// Shared operation input for portable contract checks and physical-adapter execution.
pub fn enqueue_retry(fence: Fence) -> [Request; 2] {
    let request = Request {
        fence,
        operation_id: 1,
        action: Action::Enqueue(Pending {
            sender_id: "sender".into(),
            client_id: "stable".into(),
            conversation_id: "conversation".into(),
            payload: Blob(vec![0, 255, 42]),
        }),
    };
    [request.clone(), request]
}

use newim_sdk_core::connection::{AuthCause, Connection, Effect, Event, Status};
use newim_sdk_core::push::open_notification;
use newim_sdk_core::store::Fence;

fn connection() -> Connection {
    Connection::new(
        Fence {
            account: "account".into(),
            instance: "device".into(),
            generation: 1,
        },
        0,
    )
    .unwrap()
}

#[test]
fn a_notification_cannot_sign_in_or_advance_a_store_fence() {
    let mut client = connection();
    assert!(
        open_notification(&mut client, "sync", 1, 0)
            .unwrap()
            .is_empty()
    );
    let [
        Effect::RecoverCredentials {
            ticket,
            cause: AuthCause::Missing,
        },
    ]: [Effect; 1] = client
        .update(Event::NetworkAvailable, 2, 0)
        .unwrap()
        .try_into()
        .unwrap()
    else {
        panic!("expected normal authentication");
    };
    assert!(
        open_notification(&mut client, "sync", 3, 0)
            .unwrap()
            .is_empty()
    );
    let effects = client.update(Event::Logout, 4, 0).unwrap();
    assert_eq!(
        effects,
        vec![
            Effect::Cancel(ticket.clone()),
            Effect::AdvanceFence(ticket.fence.clone())
        ]
    );
    assert!(
        open_notification(&mut client, "sync", 5, 0)
            .unwrap()
            .is_empty()
    );
    let mut next = ticket.fence.clone();
    next.generation += 1;
    client
        .update(
            Event::FenceAdvanced {
                previous: &ticket.fence,
                next,
            },
            6,
            0,
        )
        .unwrap();
    assert_eq!(client.status(), Status::Closed);
    assert!(
        open_notification(&mut client, "sync", 7, 0)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn only_the_generic_sync_marker_requests_a_fenced_refresh() {
    let mut client = connection();
    let [Effect::RecoverCredentials { ticket, .. }]: [Effect; 1] = client
        .update(Event::NetworkAvailable, 1, 0)
        .unwrap()
        .try_into()
        .unwrap()
    else {
        panic!("expected normal authentication");
    };
    assert_eq!(
        client
            .update(
                Event::Credentials {
                    ticket: &ticket,
                    expires_at_ms: 10
                },
                2,
                0
            )
            .unwrap(),
        vec![Effect::LoadDirectory(ticket.clone())]
    );
    assert!(
        open_notification(&mut client, "room_id", 3, 0)
            .unwrap()
            .is_empty()
    );
    let effects = open_notification(&mut client, "sync", 4, 0).unwrap();
    let [Effect::Cancel(old), Effect::LoadDirectory(current)] = effects.as_slice() else {
        panic!("tap must trigger normal directory sync");
    };
    assert_eq!(old, &ticket);
    assert_eq!(current.fence, ticket.fence);
    assert!(!client.accepts(old));
    assert!(client.accepts(current));
}

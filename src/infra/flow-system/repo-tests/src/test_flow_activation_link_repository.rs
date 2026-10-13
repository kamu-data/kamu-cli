// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Duration, DurationRound, Utc};
use dill::Catalog;
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_links_initially(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn FlowActivationLinkRepository>()
        .unwrap();

    let links = repo
        .get_downstream_links(&[FlowID::new(1), FlowID::new(2)])
        .await
        .unwrap();
    assert_eq!(links, vec![]);

    let links = repo.get_downstream_links(&[]).await.unwrap();
    assert_eq!(links, vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_and_get_link(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn FlowActivationLinkRepository>()
        .unwrap();

    let link = make_link(1, 2, start_time());
    repo.save_link(&link).await.unwrap();

    let links = repo.get_downstream_links(&[FlowID::new(1)]).await.unwrap();
    assert_eq!(links, vec![link]);

    // A downstream flow does not see the link as its own downstream
    let links = repo.get_downstream_links(&[FlowID::new(2)]).await.unwrap();
    assert_eq!(links, vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_link_is_idempotent(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn FlowActivationLinkRepository>()
        .unwrap();

    let link = make_link(1, 2, start_time());
    repo.save_link(&link).await.unwrap();
    repo.save_link(&link).await.unwrap();

    // Saving the same pair again keeps the original activation time
    repo.save_link(&make_link(1, 2, start_time() + Duration::minutes(5)))
        .await
        .unwrap();

    let links = repo.get_downstream_links(&[FlowID::new(1)]).await.unwrap();
    assert_eq!(links, vec![link]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_one_upstream_activates_many_downstreams(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn FlowActivationLinkRepository>()
        .unwrap();

    let t = start_time();
    let link_to_4 = make_link(1, 4, t + Duration::seconds(2));
    let link_to_3 = make_link(1, 3, t + Duration::seconds(1));
    let link_to_2 = make_link(1, 2, t + Duration::seconds(2));
    for link in [&link_to_4, &link_to_3, &link_to_2] {
        repo.save_link(link).await.unwrap();
    }

    // Ordered by activation time, then by downstream flow
    let links = repo.get_downstream_links(&[FlowID::new(1)]).await.unwrap();
    assert_eq!(links, vec![link_to_3, link_to_2, link_to_4]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_many_upstreams_activate_one_downstream(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn FlowActivationLinkRepository>()
        .unwrap();

    let t = start_time();
    let link_from_1 = make_link(1, 5, t);
    let link_from_2 = make_link(2, 5, t + Duration::seconds(1));
    repo.save_link(&link_from_1).await.unwrap();
    repo.save_link(&link_from_2).await.unwrap();

    let links = repo.get_downstream_links(&[FlowID::new(1)]).await.unwrap();
    assert_eq!(links, vec![link_from_1]);

    let links = repo.get_downstream_links(&[FlowID::new(2)]).await.unwrap();
    assert_eq!(links, vec![link_from_2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_links_of_several_upstreams(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn FlowActivationLinkRepository>()
        .unwrap();

    let t = start_time();
    let link_3_to_4 = make_link(3, 4, t);
    let link_1_to_5 = make_link(1, 5, t + Duration::seconds(1));
    let link_1_to_2 = make_link(1, 2, t + Duration::seconds(2));
    let link_6_to_7 = make_link(6, 7, t);
    for link in [&link_3_to_4, &link_1_to_5, &link_1_to_2, &link_6_to_7] {
        repo.save_link(link).await.unwrap();
    }

    // Grouped by upstream flow; unknown upstream flows yield nothing
    let links = repo
        .get_downstream_links(&[FlowID::new(3), FlowID::new(1), FlowID::new(100)])
        .await
        .unwrap();
    assert_eq!(links, vec![link_1_to_5, link_1_to_2, link_3_to_4]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn start_time() -> DateTime<Utc> {
    // Storage keeps sub-second precision differently, whole seconds compare
    // reliably
    Utc::now().duration_round(Duration::seconds(1)).unwrap()
}

fn make_link(upstream: u64, downstream: u64, activated_at: DateTime<Utc>) -> FlowActivationLink {
    FlowActivationLink {
        upstream_flow_id: FlowID::new(upstream),
        downstream_flow_id: FlowID::new(downstream),
        activated_at,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
